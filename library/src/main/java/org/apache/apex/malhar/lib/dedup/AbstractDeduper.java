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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.apex.malhar.lib.state.managed.ManagedTimeUnifiedStateImpl;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.netlet.util.Slice;

/**
 * An operator that de-dupes a stream.
 *
 * @param <T> type of events
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractDeduper<T> implements Operator, Operator.CheckpointNotificationListener,
    Operator.IdleTimeHandler
{
  @NotNull
  protected final ManagedTimeUnifiedStateImpl managedState = new ManagedTimeUnifiedStateImpl();

  private transient long sleepMillis;

  private transient Map<T, Future<Slice>> waitingEvents = Maps.newLinkedHashMap();

  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<>();

  public final transient DefaultOutputPort<T> duplicates = new DefaultOutputPort<>();

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      long time = getTime(tuple);
      Future<Slice> valFuture = managedState.getAsync(time, getKey(tuple));
      if (valFuture.isDone()) {
        try {
          processEvent(tuple, valFuture.get());
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException("process", e);
        }
      } else {
        waitingEvents.put(tuple, valFuture);
      }
    }
  };

  protected abstract long getTime(T tuple);

  protected abstract Slice getKey(T tuple);

  @Override
  public void setup(Context.OperatorContext context)
  {
    sleepMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
    managedState.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    managedState.beginWindow(windowId);
  }

  @Override
  public void handleIdleTime()
  {
    if (waitingEvents.size() > 0) {
      Iterator<Map.Entry<T, Future<Slice>>> waitIterator = waitingEvents.entrySet().iterator();
      while (waitIterator.hasNext()) {
        Map.Entry<T, Future<Slice>> waitingEvent = waitIterator.next();
        if (waitingEvent.getValue().isDone()) {
          try {
            processEvent(waitingEvent.getKey(), waitingEvent.getValue().get());
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("handle idle time", e);
          }
          waitIterator.remove();
        }
      }
    } else {
      /* nothing to do here, so sleep for a while to avoid busy loop */
      try {
        Thread.sleep(sleepMillis);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
  }

  protected void processEvent(T tuple, Slice value)
  {
    if (value == BucketedState.EXPIRED) {
      return;
    }
    if (value == null) {
      //not a duplicate event
      output.emit(tuple);
    } else {
      if (duplicates.isConnected()) {
        duplicates.emit(tuple);
      }
    }
  }

  @Override
  public void endWindow()
  {
    Iterator<Map.Entry<T, Future<Slice>>> waitIterator = waitingEvents.entrySet().iterator();
    while (waitIterator.hasNext()) {
      Map.Entry<T, Future<Slice>> waitingEvent = waitIterator.next();
      try {
        processEvent(waitingEvent.getKey(), waitingEvent.getValue().get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("end window", e);
      }
      waitIterator.remove();

    }
    managedState.endWindow();
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
}
