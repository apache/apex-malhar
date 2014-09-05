/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.distributeddistinct;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.IdleTimeHandler;
import com.datatorrent.api.Partitioner;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.algo.UniqueValueCount.InternalCountOutput;
import com.datatorrent.lib.bucket.Bucket;
import com.datatorrent.lib.bucket.BucketManager;
import com.datatorrent.lib.bucket.BucketManagerImpl;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.util.KeyValPair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class HDFSUniqueValueCountAppender<V> extends BaseOperator implements IdleTimeHandler, Partitioner<HDFSUniqueValueCountAppender<V>>
{
  class BucketListener implements BucketManager.Listener<BucketableInternalCountOutput<V>>
  {
    @Override
    public void bucketOffLoaded(long bucketKey)
    {
    }

    @Override
    public void bucketLoaded(Bucket<BucketableInternalCountOutput<V>> bucket)
    {
      fetchedBuckets.add(bucket);
    }
  }

  protected Set<Integer> partitionKeys;
  protected int partitionMask;
  protected transient long windowID;
  protected BucketManagerImpl<BucketableInternalCountOutput<V>> bucketManager;
  protected Set<V> keySet;
  protected transient Map<Long, Set<BucketableInternalCountOutput<V>>> waitingEvents;
  protected transient final BlockingQueue<Bucket<BucketableInternalCountOutput<V>>> fetchedBuckets;
  protected transient Map<Long, Set<V>> waitingOutputBuckets;
  private transient long sleepTimeMillis;
  boolean emittingTuples;

  public HDFSUniqueValueCountAppender()
  {
    partitionKeys = Sets.newHashSet(0);
    partitionMask = 0;
    fetchedBuckets = new LinkedBlockingQueue<Bucket<BucketableInternalCountOutput<V>>>();
    keySet = new HashSet<V>();
  }

  public void setBucketManager(BucketManagerImpl<BucketableInternalCountOutput<V>> bucketManager)
  {
    this.bucketManager = bucketManager;
  }

  public final transient DefaultInputPort<InternalCountOutput<V>> input = new DefaultInputPort<InternalCountOutput<V>>() {
    @Override
    public void process(InternalCountOutput<V> tuple)
    {
      processTuple(tuple);
    }
  };

  public final transient DefaultOutputPort<KeyValPair<Object, Object>> output = new DefaultOutputPort<KeyValPair<Object, Object>>();

  protected void processTuple(InternalCountOutput<V> tuple)
  {
    emittingTuples = false;
    BucketableInternalCountOutput<V> bucketableTuple = new BucketableInternalCountOutput<V>(tuple.getKey(), tuple.getValue(), tuple.getInternalSet());
    Object key = bucketableTuple.getEventKey();
    keySet.add(tuple.getKey());
    long bucketKey = bucketManager.getBucketKeyFor(bucketableTuple);
    Bucket<BucketableInternalCountOutput<V>> bucket = bucketManager.getBucket(bucketKey);
    if (bucket == null) {
      bucketManager.loadBucketData(bucketKey);
      Set<BucketableInternalCountOutput<V>> tempSet = waitingEvents.get(bucketKey);
      if (tempSet == null) {
        tempSet = new HashSet<BucketableInternalCountOutput<V>>();
      }
      tempSet.add(bucketableTuple);
      waitingEvents.put(bucketKey, tempSet);
    } else {
      BucketableInternalCountOutput<V> unwritten = bucket.getValueFromUnwrittenPart(key);
      BucketableInternalCountOutput<V> written = bucket.getValueFromWrittenPart(key);
      Set<Object> tempSet = bucketableTuple.getInternalSet();
      if (written != null)
        tempSet.addAll(written.getInternalSet());
      if (unwritten != null)
        tempSet.addAll(unwritten.getInternalSet());
      bucketManager.newEvent(bucketKey, new BucketableInternalCountOutput<V>(bucketableTuple.getKey(), tempSet.size(), tempSet));
    }
  }

  @Override
  public void endWindow()
  {
    try {
      bucketManager.blockUntilAllRequestsServiced();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    handleIdleTime();
    bucketManager.endWindow(windowID);
    emittingTuples = true;
    for (V key : keySet) {
      long bucketKey = bucketManager.getBucketKeyFor(new BucketableInternalCountOutput<V>(key, null, null));
      Bucket<BucketableInternalCountOutput<V>> bucket = bucketManager.getBucket(bucketKey);
      if (bucket != null) {
        BucketableInternalCountOutput<V> countOutput = bucket.getValueFromWrittenPart(key);
        output.emit(new KeyValPair<Object, Object>(countOutput.getKey(), countOutput.getValue()));
      } else {
        Set<V> tempSet;
        if (waitingOutputBuckets.get(key) == null) {
          tempSet = new HashSet<V>();
        } else {
          tempSet = waitingOutputBuckets.get(bucketKey);
        }
        tempSet.add(key);
        waitingOutputBuckets.put(bucketKey, tempSet);
      }
    }
    try {
      bucketManager.blockUntilAllRequestsServiced();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    LOG.debug("Handle Idle Time is called to emit buckets from unloaded buckets");
    handleIdleTime();
    waitingOutputBuckets.clear();
  }

  @Override
  public void beginWindow(long windowID)
  {
    this.windowID = windowID;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    ((HdfsBucketStore<BucketableInternalCountOutput<V>>) bucketManager.getBucketStore()).setConfiguration(context.getId(), context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
    windowID = context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID);
    bucketManager.startService(new BucketListener());
    waitingEvents = new HashMap<Long, Set<BucketableInternalCountOutput<V>>>();
    sleepTimeMillis = context.getValue(OperatorContext.SPIN_MILLIS);
    waitingOutputBuckets = new HashMap<Long, Set<V>>();
  }

  @Override
  public void handleIdleTime()
  {
    LOG.debug("Handle Idle Time is called");
    if (fetchedBuckets.isEmpty()) {
      /* nothing to do here, so sleep for a while to avoid busy loop */
      try {
        Thread.sleep(sleepTimeMillis);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    } else if (!emittingTuples) {
      LOG.debug("Buckets are being fetched");
      /**
       * Remove all the events from waiting list whose buckets are loaded. Process these events again.
       */
      Bucket<BucketableInternalCountOutput<V>> bucket;
      while ((bucket = fetchedBuckets.poll()) != null) {
        Set<BucketableInternalCountOutput<V>> waitingSet = waitingEvents.remove(bucket.bucketKey);
        if (waitingSet != null) {
          for (BucketableInternalCountOutput<V> bucketableTuple : waitingSet) {
            Object key = bucketableTuple.getEventKey();
            long bucketKey = bucketManager.getBucketKeyFor(bucketableTuple);
            BucketableInternalCountOutput<V> unwritten = bucket.getValueFromUnwrittenPart(key);
            BucketableInternalCountOutput<V> written = bucket.getValueFromWrittenPart(key);
            Set<Object> tempSet = bucketableTuple.getInternalSet();
            if (written != null)
              tempSet.addAll(written.getInternalSet());
            if (unwritten != null)
              tempSet.addAll(unwritten.getInternalSet());
            bucketManager.newEvent(bucketKey, new BucketableInternalCountOutput<V>(bucketableTuple.getKey(), tempSet.size(), tempSet));
          }
        }
      }
    } else {
      Bucket<BucketableInternalCountOutput<V>> bucket;
      while ((bucket = fetchedBuckets.poll()) != null) {
        LOG.debug("Buckets loaded during endwindow");
        Set<V> tempKeySet = waitingOutputBuckets.get(bucket.bucketKey);
        for (V key : tempKeySet) {
          BucketableInternalCountOutput<V> countOutput = bucket.getValueFromWrittenPart(key);
          output.emit(new KeyValPair<Object, Object>(countOutput.getKey(), countOutput.getValue()));
        }
      }
    }
  }

  /**
   * Assigns the partitions according to certain key values and keeps track of the keys that each partition will be
   * processing so that in the case of a rollback, each partition will only clear the data that it is responsible for.
   */
  @Override
  public Collection<com.datatorrent.api.Partitioner.Partition<HDFSUniqueValueCountAppender<V>>> definePartitions(Collection<com.datatorrent.api.Partitioner.Partition<HDFSUniqueValueCountAppender<V>>> partitions, int incrementalCapacity)
  {
    if (incrementalCapacity == 0) {
      return partitions;
    }

    final int finalCapacity = partitions.size() + incrementalCapacity;
    HDFSUniqueValueCountAppender<V> anOldOperator = partitions.iterator().next().getPartitionedInstance();
    partitions.clear();

    Collection<Partition<HDFSUniqueValueCountAppender<V>>> newPartitions = Lists.newArrayListWithCapacity(finalCapacity);

    for (int i = 0; i < finalCapacity; i++) {
      try {
        @SuppressWarnings("unchecked")
        HDFSUniqueValueCountAppender<V> statefulUniqueCount = this.getClass().newInstance();
        DefaultPartition<HDFSUniqueValueCountAppender<V>> partition = new DefaultPartition<HDFSUniqueValueCountAppender<V>>(statefulUniqueCount);
        newPartitions.add(partition);
      } catch (Throwable cause) {
        DTThrowable.rethrow(cause);
      }
    }

    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(newPartitions), input);
    int lPartitionMask = newPartitions.iterator().next().getPartitionKeys().get(input).mask;

    for (Partition<HDFSUniqueValueCountAppender<V>> statefulUniqueCountPartition : newPartitions) {
      HDFSUniqueValueCountAppender<V> statefulUniqueCountInstance = statefulUniqueCountPartition.getPartitionedInstance();

      statefulUniqueCountInstance.partitionKeys = statefulUniqueCountPartition.getPartitionKeys().get(input).partitions;
      statefulUniqueCountInstance.partitionMask = lPartitionMask;
      statefulUniqueCountInstance.bucketManager = anOldOperator.bucketManager;
    }
    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, com.datatorrent.api.Partitioner.Partition<HDFSUniqueValueCountAppender<V>>> arg0)
  {

  }
  
  private static final Logger LOG = LoggerFactory.getLogger(RandomKeyValGenerator.class);
}
