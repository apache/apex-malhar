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
package org.apache.apex.malhar.lib.state.managed;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fileaccess.FileAccess;
import org.apache.apex.malhar.lib.state.BucketedState;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * In this implementation of {@link AbstractManagedStateImpl} the buckets in memory are time-buckets.
 *
 * @since 3.4.0
 */
public class ManagedTimeUnifiedStateImpl extends AbstractManagedStateImpl implements BucketedState
{
  private final transient LinkedBlockingQueue<Long> purgedTimeBuckets = Queues.newLinkedBlockingQueue();
  private final transient Set<Bucket> bucketsForTeardown = Sets.newHashSet();

  public ManagedTimeUnifiedStateImpl()
  {
    bucketsFileSystem = new TimeUnifiedBucketsFileSystem();
  }

  @Override
  public long getNumBuckets()
  {
    return timeBucketAssigner.getNumBuckets();
  }

  @Override
  public void put(long time, @NotNull Slice key, @NotNull Slice value)
  {
    long timeBucket = timeBucketAssigner.getTimeBucket(time);
    putInBucket(timeBucket, timeBucket, key, value);
  }

  @Override
  public Slice getSync(long time, @NotNull Slice key)
  {
    long timeBucket = timeBucketAssigner.getTimeBucket(time);
    if (timeBucket == -1) {
      //time is expired so return expired slice.
      return BucketedState.EXPIRED;
    }
    return getValueFromBucketSync(timeBucket, timeBucket, key);
  }

  @Override
  public Future<Slice> getAsync(long time, @NotNull Slice key)
  {
    long timeBucket = timeBucketAssigner.getTimeBucket(time);
    if (timeBucket == -1) {
      //time is expired so return expired slice.
      return Futures.immediateFuture(BucketedState.EXPIRED);
    }
    return getValueFromBucketAsync(timeBucket, timeBucket, key);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    Long purgedTimeBucket;

    //collect all the purged time buckets
    while (null != (purgedTimeBucket = purgedTimeBuckets.poll())) {
      long purgedTimeBucketIdx = getBucketIdx(purgedTimeBucket);
      if (buckets.containsKey(purgedTimeBucketIdx) && buckets.get(purgedTimeBucketIdx).getBucketId() == purgedTimeBucket) {
        bucketsForTeardown.add(buckets.get(purgedTimeBucketIdx));
        buckets.remove(purgedTimeBucketIdx);
      }
    }

    //tear down all the eligible time buckets
    Iterator<Bucket> bucketIterator = bucketsForTeardown.iterator();
    while (bucketIterator.hasNext()) {
      Bucket bucket = bucketIterator.next();
      if (!tasksPerBucketId.containsKey(bucket.getBucketId())) {
        //no pending asynchronous queries for this bucket id
        bucket.teardown();
        bucketIterator.remove();
      }
    }
  }

  @Override
  protected void handleBucketConflict(long bucketId, long newBucketId)
  {
    Preconditions.checkArgument(buckets.get(bucketId).getBucketId() < newBucketId, "new time bucket should have a value"
        + " greater than the old time bucket");
    //Time buckets are purged periodically so here a bucket conflict is expected and so we just ignore conflicts.
    bucketsForTeardown.add(buckets.get(bucketId));
    buckets.put(bucketId, newBucket(newBucketId));
    buckets.get(bucketId).setup(this);
  }

  @Override
  public void purgeTimeBucketsLessThanEqualTo(long timeBucket)
  {
    purgedTimeBuckets.add(timeBucket);
    super.purgeTimeBucketsLessThanEqualTo(timeBucket);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    // set UnboundedTimeBucketAssigner to this managed state impl
    if (timeBucketAssigner == null) {
      UnboundedTimeBucketAssigner unboundedTimeBucketAssigner = new UnboundedTimeBucketAssigner();
      setTimeBucketAssigner(unboundedTimeBucketAssigner);
    }
    super.setup(context);
  }

  /**
   * This uses operator id instead of bucket id as the name of parent folder of time-buckets. This is because
   * multiple partitions may work on same time-buckets.
   */
  private static class TimeUnifiedBucketsFileSystem extends BucketsFileSystem
  {
    @Override
    protected FileAccess.FileWriter getWriter(long bucketId, String fileName) throws IOException
    {
      return managedStateContext.getFileAccess().getWriter(managedStateContext.getOperatorContext().getId(), fileName);
    }

    @Override
    protected FileAccess.FileReader getReader(long bucketId, String fileName) throws IOException
    {
      return managedStateContext.getFileAccess().getReader(managedStateContext.getOperatorContext().getId(), fileName);
    }

    @Override
    protected void rename(long bucketId, String fromName, String toName) throws IOException
    {
      managedStateContext.getFileAccess().rename(managedStateContext.getOperatorContext().getId(), fromName, toName);
    }

    @Override
    protected DataOutputStream getOutputStream(long bucketId, String fileName) throws IOException
    {
      return managedStateContext.getFileAccess().getOutputStream(managedStateContext.getOperatorContext().getId(),
          fileName);
    }

    @Override
    protected DataInputStream getInputStream(long bucketId, String fileName) throws IOException
    {
      return managedStateContext.getFileAccess().getInputStream(managedStateContext.getOperatorContext().getId(),
          fileName);
    }

    @Override
    protected boolean exists(long bucketId, String fileName) throws IOException
    {
      return managedStateContext.getFileAccess().exists(managedStateContext.getOperatorContext().getId(),
          fileName);
    }

    @Override
    protected RemoteIterator<LocatedFileStatus> listFiles(long bucketId) throws IOException
    {
      return managedStateContext.getFileAccess().listFiles(managedStateContext.getOperatorContext().getId());
    }

    @Override
    protected void delete(long bucketId, String fileName) throws IOException
    {
      managedStateContext.getFileAccess().delete(managedStateContext.getOperatorContext().getId(), fileName);
    }

    @Override
    protected void deleteBucket(long bucketId) throws IOException
    {
      managedStateContext.getFileAccess().deleteBucket(managedStateContext.getOperatorContext().getId());
    }

    @Override
    protected void addBucketName(long bucketId)
    {
      long operatorId = managedStateContext.getOperatorContext().getId();
      if (!bucketNamesOnFS.contains(operatorId)) {
        bucketNamesOnFS.add(operatorId);
      }
    }
  }

  private static transient Logger LOG = LoggerFactory.getLogger(ManagedTimeUnifiedStateImpl.class);
}
