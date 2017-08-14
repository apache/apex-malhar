/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.dedup;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fileaccess.FileAccessFSImpl;
import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.apex.malhar.lib.state.managed.AbstractManagedStateImpl;
import org.apache.apex.malhar.lib.state.managed.ManagedTimeUnifiedStateImpl;
import org.apache.apex.malhar.lib.state.managed.MovingBoundaryTimeBucketAssigner;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.netlet.util.Slice;

/**
 * Abstract class which allows de-duplicating incoming tuples based on a configured key.
 * Also supports expiry mechanism based on a configurable expiry period configured using {@link MovingBoundaryTimeBucketAssigner}
 * in {@link ManagedTimeUnifiedStateImpl}
 * Following steps are used in identifying the state of a particular tuple:
 * 1. Check if the time of the tuple falls in an expired bucket. If so, the tuple is expired
 * 2. If the tuple is a valid event, it is checked in the store whether the same key already exists in the
 * time bucket identified by the event time. If, so, the tuple is a duplicate.
 * 3. Otherwise the tuple is a unique tuple.
 *
 * @param <T> type of events
 *
 * @since 3.5.0
 */
@Evolving
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractDeduper<T>
    implements Operator, Operator.IdleTimeHandler, ActivationListener<Context>, Operator.CheckpointNotificationListener
{

  private static final String BUCKET_DIR = "bucket_data";

  /**
   * The input port on which events are received.
   */
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public final void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  /**
   * The output port on which deduped events are emitted.
   */
  public final transient DefaultOutputPort<T> unique = new DefaultOutputPort<>();

  /**
   * The output port on which duplicate events are emitted.
   */
  public final transient DefaultOutputPort<T> duplicate = new DefaultOutputPort<>();

  /**
   * The output port on which expired events are emitted.
   */
  public final transient DefaultOutputPort<T> expired = new DefaultOutputPort<>();

  /**
   * Whether or not the order of tuples be maintained.
   * Making this "true" might entail some cost in performance, but makes the operator idempotent.
   */
  private boolean preserveTupleOrder = true;

  @NotNull
  protected AbstractManagedStateImpl managedState;

  /**
   * Map to hold the result of a tuple processing (unique, duplicate, expired or error) until previous
   * tuples get processed. This is used only when {@link #preserveTupleOrder} is true.
   */
  private transient Map<T, Decision> decisions;
  private transient Map<T, Future<Slice>> waitingEvents = Maps.newLinkedHashMap();
  private transient Map<Slice, Long> asyncEvents = Maps.newLinkedHashMap();

  // Metrics
  @AutoMetric
  private transient long uniqueEvents;
  @AutoMetric
  private transient long duplicateEvents;
  @AutoMetric
  private transient long expiredEvents;

  @Override
  public void setup(OperatorContext context)
  {
    ((FileAccessFSImpl)managedState.getFileAccess()).setBasePath(context.getValue(DAG.APPLICATION_PATH)
        + Path.SEPARATOR + BUCKET_DIR);
    managedState.setup(context);

    if (preserveTupleOrder) {
      decisions = Maps.newLinkedHashMap();
    }
  }

  @Override
  public void beginWindow(long l)
  {
    // Reset Metrics
    uniqueEvents = 0;
    duplicateEvents = 0;
    expiredEvents = 0;

    managedState.beginWindow(l);
  }

  protected abstract Slice getKey(T event);

  protected abstract long getTime(T event);

  /**
   * Processes an incoming tuple
   *
   * @param tuple the incoming tuple
   */
  protected void processTuple(T tuple)
  {
    Future<Slice> valFuture = getAsyncManagedState(tuple);

    if (valFuture.isDone()) {
      try {
        processEvent(tuple, valFuture.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    } else {
      processWaitingEvent(tuple, valFuture);
    }
  }

  /**
   * Processes a looked-up event
   *
   * @param tuple the incoming tuple
   * @param value the looked up key of the tuple
   */
  protected void processEvent(T tuple, Slice value)
  {
    if (value == BucketedState.EXPIRED) {
      processInvalid(tuple);
      return;
    }
    processValid(tuple, value);
  }

  /**
   * Processes a tuple which is waiting for the lookup to return.
   *
   * @param tuple The tuple which needs to wait
   * @param future The future object which will ultimately return the lookup result
   */
  protected void processWaitingEvent(T tuple, Future<Slice> future)
  {
    waitingEvents.put(tuple, future);
    if (preserveTupleOrder) {
      recordDecision(tuple, Decision.UNKNOWN);
    }
  }

  /**
   * Processes a valid (non-expired) tuple. This tuple may be a unique or a duplicate.
   *
   * @param tuple
   *          The tuple to be processed
   * @param value
   *          Looked up key of the tuple
   */
  protected void processValid(T tuple, Slice value)
  {
    if (!preserveTupleOrder || waitingEvents.isEmpty()) {
      if (value == null) {
        putManagedState(tuple);
        processUnique(tuple);
      } else {
        processDuplicate(tuple);
      }
    } else {
      processWaitingEvent(tuple, Futures.immediateFuture(value));
    }
  }

  /**
   * Processes invalid tuples.
   *
   * @param tuple the incoming tuple
   */
  protected void processInvalid(T tuple)
  {
    if (preserveTupleOrder && !decisions.isEmpty()) {
      recordDecision(tuple, Decision.EXPIRED);
    } else {
      processExpired(tuple);
    }
  }

  /**
   * Processes an expired tuple
   *
   * @param tuple the incoming tuple
   */
  protected void processExpired(T tuple)
  {
    expiredEvents++;
    emitExpired(tuple);
  }

  /**
   * Processes the duplicate tuple.
   *
   * @param tuple
   *          The tuple which is a duplicate
   */
  protected void processDuplicate(T tuple)
  {
    if (preserveTupleOrder && !decisions.isEmpty()) {
      recordDecision(tuple, Decision.DUPLICATE);
    } else {
      duplicateEvents++;
      emitDuplicate(tuple);
    }
  }

  /**
   * Processes the unique tuple.
   *
   * @param tuple
   *          The tuple which is a unique
   */
  protected void processUnique(T tuple)
  {
    if (preserveTupleOrder && !decisions.isEmpty()) {
      recordDecision(tuple, Decision.UNIQUE);
    } else {
      uniqueEvents++;
      emitUnique(tuple);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void handleIdleTime()
  {
    if (preserveTupleOrder) {
      emitProcessedTuples();
    }
    processAuxiliary(false);
  }

  /**
   * Does any auxiliary processing in the idle time of the operator.
   * Processes any tuples which are waiting for the lookup to return.
   *
   * @param finalize Whether or not to wait for future to return
   */
  protected void processAuxiliary(boolean finalize)
  {
    if (waitingEvents.size() > 0) {
      Iterator<Map.Entry<T, Future<Slice>>> waitIterator = waitingEvents.entrySet().iterator();
      while (waitIterator.hasNext()) {
        Map.Entry<T, Future<Slice>> waitingEvent = waitIterator.next();
        T tuple = waitingEvent.getKey();
        Slice tupleKey = getKey(tuple);
        long tupleTime = getTime(tuple);
        Future<Slice> future = waitingEvent.getValue();
        if (future.isDone() || finalize ) {
          try {
            Long asyncEventsTupleTime = asyncEvents.get(tupleKey);
            if (future.get() == null && (asyncEventsTupleTime == null || asyncEventsTupleTime < tupleTime) ) {
              putManagedState(tuple);
              asyncEvents.put(tupleKey, tupleTime);
              processUnique(tuple);
            } else {
              processDuplicate(tuple);
            }
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("handle idle time", e);
          }
          waitIterator.remove();
        }
        if (!finalize) {
          break;
        }
      }
    }
  }

  @Override
  public void endWindow()
  {
    processAuxiliary(true);
    if (preserveTupleOrder) {
      emitProcessedTuples();
    }
    Preconditions.checkArgument(waitingEvents.isEmpty());
    asyncEvents.clear();
    managedState.endWindow();
  }

  protected abstract Future<Slice> getAsyncManagedState(T tuple);

  protected abstract void putManagedState(T tuple);

  /**
   * Records a decision for use later. This is needed to ensure that the order of incoming tuples is maintained.
   *
   * @param tuple the incoming tuple
   * @param d The decision for the tuple
   */
  protected void recordDecision(T tuple, Decision d)
  {
    decisions.put(tuple, d);
  }

  /**
   * Processes tuples for which the decision (unique / duplicate / expired) has been made.
   * Breaks once an undecided tuple is found, as we don't want to emit out of order
   */
  protected void emitProcessedTuples()
  {
    Iterator<Entry<T, Decision>> entries = decisions.entrySet().iterator();
    while (entries.hasNext()) {
      Entry<T, Decision> td = entries.next();
      switch (td.getValue()) {
        case UNIQUE:
          uniqueEvents++;
          emitUnique(td.getKey());
          entries.remove();
          break;
        case DUPLICATE:
          duplicateEvents++;
          emitDuplicate(td.getKey());
          entries.remove();
          break;
        case EXPIRED:
          expiredEvents++;
          emitExpired(td.getKey());
          entries.remove();
          break;
        default:
          /*
           * Decision for this is still UNKNOWN. Tuple is still waiting for bucket to be loaded. Break.
           */
          break;
      }
    }
  }

  @Override
  public void teardown()
  {
    managedState.teardown();
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    managedState.beforeCheckpoint(windowId);
  }

  @Override
  public void checkpointed(long windowId)
  {
    managedState.checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
    managedState.committed(windowId);
  }

  protected void emitUnique(T event)
  {
    unique.emit(event);
  }

  protected void emitDuplicate(T event)
  {
    duplicate.emit(event);
  }

  protected void emitExpired(T event)
  {
    expired.emit(event);
  }

  /**
   * Checks whether output of deduper should preserve the input order
   */
  public boolean isOrderedOutput()
  {
    return preserveTupleOrder;
  }

  /**
   * If set to true, the deduper will emit tuples in the order in which they were received. Tuples which arrived later
   * will wait for previous tuples to get processed and emitted. If not set, the order of tuples may change as tuples
   * may be emitted out of order as and when they get processed.
   *
   * @param preserveTupleOrder whether or not to preserve the order of incoming tuples
   */
  public void setPreserveTupleOrder(boolean preserveTupleOrder)
  {
    this.preserveTupleOrder = preserveTupleOrder;
  }

  /**
   * Enum for holding all possible values for a decision for a tuple
   */
  protected enum Decision
  {
    UNIQUE, DUPLICATE, EXPIRED, UNKNOWN
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractDeduper.class);
}
