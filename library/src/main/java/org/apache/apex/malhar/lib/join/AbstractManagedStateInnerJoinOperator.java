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
package org.apache.apex.malhar.lib.join;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.fileaccess.FileAccessFSImpl;
import org.apache.apex.malhar.lib.state.managed.ManagedTimeStateImpl;
import org.apache.apex.malhar.lib.state.managed.ManagedTimeStateMultiValue;
import org.apache.apex.malhar.lib.state.managed.MovingBoundaryTimeBucketAssigner;
import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.hadoop.fs.Path;
import com.google.common.collect.Maps;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

/**
 * An abstract implementation of inner join operator over Managed state which extends from
 * AbstractInnerJoinOperator.
 *
 * <b>Properties:</b><br>
 * <b>noOfBuckets</b>: Number of buckets required for Managed state. <br>
 * <b>bucketSpanTime</b>: Indicates the length of the time bucket. <br>
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractManagedStateInnerJoinOperator<K,T> extends AbstractInnerJoinOperator<K,T> implements
    Operator.CheckpointNotificationListener, Operator.IdleTimeHandler
{
  public static final String stateDir = "managedState";
  public static final String stream1State = "stream1Data";
  public static final String stream2State = "stream2Data";
  private transient Map<JoinEvent<K,T>, Future<List>> waitingEvents = Maps.newLinkedHashMap();
  private int noOfBuckets = 1;
  private Long bucketSpanTime;
  protected ManagedTimeStateImpl stream1Store;
  protected ManagedTimeStateImpl stream2Store;

  /**
   * Create Managed states and stores for both the streams.
   */
  @Override
  public void createStores()
  {
    stream1Store = new ManagedTimeStateImpl();
    stream2Store = new ManagedTimeStateImpl();
    stream1Store.setNumBuckets(noOfBuckets);
    stream2Store.setNumBuckets(noOfBuckets);
    assert stream1Store.getTimeBucketAssigner() == stream2Store.getTimeBucketAssigner();
    if (bucketSpanTime != null) {
      stream1Store.getTimeBucketAssigner().setBucketSpan(Duration.millis(bucketSpanTime));
    }
    if (stream1Store.getTimeBucketAssigner() instanceof MovingBoundaryTimeBucketAssigner) {
      ((MovingBoundaryTimeBucketAssigner)stream1Store.getTimeBucketAssigner()).setExpireBefore(Duration.millis(getExpiryTime()));
    }

    stream1Data = new ManagedTimeStateMultiValue(stream1Store, !isLeftKeyPrimary());
    stream2Data = new ManagedTimeStateMultiValue(stream2Store, !isRightKeyPrimary());
  }

  /**
   * Process the tuple which are received from input ports with the following steps:
   * 1) Extract key from the given tuple
   * 2) Insert <key,tuple> into the store where store is the stream1Data if the tuple
   * receives from stream1 or viceversa.
   * 3) Get the values of the key in asynchronous if found it in opposite store
   * 4) If the future is done then Merge the given tuple and values found from step (3) otherwise
   *    put it in waitingEvents
   * @param tuple given tuple
   * @param isStream1Data Specifies whether the given tuple belongs to stream1 or not.
   */
  @Override
  protected void processTuple(T tuple, boolean isStream1Data)
  {
    Spillable.SpillableListMultimap<K,T> store = isStream1Data ? stream1Data : stream2Data;
    K key = extractKey(tuple,isStream1Data);
    long timeBucket = extractTime(tuple,isStream1Data);
    if (!((ManagedTimeStateMultiValue)store).put(key, tuple,timeBucket)) {
      return;
    }
    Spillable.SpillableListMultimap<K, T> valuestore = isStream1Data ? stream2Data : stream1Data;
    Future<List> future = ((ManagedTimeStateMultiValue)valuestore).getAsync(key);
    if (future.isDone()) {
      try {
        joinStream(tuple,isStream1Data, future.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    } else {
      waitingEvents.put(new JoinEvent<>(key,tuple,isStream1Data),future);
    }
  }

  @Override
  public void handleIdleTime()
  {
    if (waitingEvents.size() > 0) {
      processWaitEvents(false);
    }
  }

  @Override
  public void beforeCheckpoint(long l)
  {
    stream1Store.beforeCheckpoint(l);
    stream2Store.beforeCheckpoint(l);
  }

  @Override
  public void checkpointed(long l)
  {
    stream1Store.checkpointed(l);
    stream2Store.checkpointed(l);
  }

  @Override
  public void committed(long l)
  {
    stream1Store.committed(l);
    stream2Store.committed(l);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    ((FileAccessFSImpl)stream1Store.getFileAccess()).setBasePath(context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + stateDir + Path.SEPARATOR + String.valueOf(context.getId()) + Path.SEPARATOR + stream1State);
    ((FileAccessFSImpl)stream2Store.getFileAccess()).setBasePath(context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + stateDir + Path.SEPARATOR + String.valueOf(context.getId()) + Path.SEPARATOR + stream2State);
    stream1Store.getCheckpointManager().setStatePath("managed_state_" + stream1State);
    stream1Store.getCheckpointManager().setStatePath("managed_state_" + stream2State);
    stream1Store.setup(context);
    stream2Store.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    stream1Store.beginWindow(windowId);
    stream2Store.beginWindow(windowId);
    super.beginWindow(windowId);
  }

  /**
   * Process the waiting events
   * @param finalize finalize Whether or not to wait for future to return
   */
  private void processWaitEvents(boolean finalize)
  {
    Iterator<Map.Entry<JoinEvent<K,T>, Future<List>>> waitIterator = waitingEvents.entrySet().iterator();
    while (waitIterator.hasNext()) {
      Map.Entry<JoinEvent<K,T>, Future<List>> waitingEvent = waitIterator.next();
      Future<List> future = waitingEvent.getValue();
      if (future.isDone() || finalize) {
        try {
          JoinEvent<K,T> event = waitingEvent.getKey();
          joinStream(event.value,event.isStream1Data,future.get());
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException("end window", e);
        }
        waitIterator.remove();
        if (!finalize) {
          break;
        }
      }
    }
  }

  @Override
  public void endWindow()
  {
    processWaitEvents(true);
    stream1Store.endWindow();
    stream2Store.endWindow();
    super.endWindow();
  }

  @Override
  public void teardown()
  {
    stream1Store.teardown();
    stream2Store.teardown();
    super.teardown();
  }

  /**
   * Return the number of buckets
   * @return the noOfBuckets
   */
  public int getNoOfBuckets()
  {
    return noOfBuckets;
  }

  /**
   * Set the number of buckets required for managed state
   * @param noOfBuckets noOfBuckets
   */
  public void setNoOfBuckets(int noOfBuckets)
  {
    this.noOfBuckets = noOfBuckets;
  }

  /**
   * Return the bucketSpanTime
   * @return the bucketSpanTime
   */
  public Long getBucketSpanTime()
  {
    return bucketSpanTime;
  }

  /**
   * Sets the length of the time bucket required for managed state.
   * @param bucketSpanTime given bucketSpanTime
   */
  public void setBucketSpanTime(Long bucketSpanTime)
  {
    this.bucketSpanTime = bucketSpanTime;
  }

  public static class JoinEvent<K,T>
  {
    public K key;
    public T value;
    public boolean isStream1Data;

    public JoinEvent(K key, T value, boolean isStream1Data)
    {
      this.key = key;
      this.value = value;
      this.isStream1Data = isStream1Data;
    }
  }
}
