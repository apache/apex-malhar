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

import java.util.concurrent.Future;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.BucketedState;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * Basic implementation of {@link AbstractManagedStateImpl} where system time corresponding to an application window is
 * used to sub-group key of a particular bucket.<br/>
 *
 * @since 3.4.0
 */
public class ManagedStateImpl extends AbstractManagedStateImpl implements BucketedState
{
  private long time = System.currentTimeMillis();
  private transient long timeIncrement;

  public ManagedStateImpl()
  {
    this.numBuckets = 1;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    timeIncrement = context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
        context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
  }

  @Override
  public void put(long bucketId, @NotNull Slice key, @NotNull Slice value)
  {
    long timeBucket = timeBucketAssigner.getTimeBucket(time);
    putInBucket(bucketId, timeBucket, key, value);
  }

  @Override
  public Slice getSync(long bucketId, @NotNull Slice key)
  {
    return getValueFromBucketSync(bucketId, -1, key);
  }

  /**
   * Returns the future using which the value is obtained.<br/>
   * If the key is present in the bucket cache, then the future has its value set when constructed;
   * if not the value is set after it's read from the data files which is after a while.
   *
   * @param key key
   * @return value of the key if found; null if the key is not found;
   */
  @Override
  public Future<Slice> getAsync(long bucketId, @NotNull Slice key)
  {
    return getValueFromBucketAsync(bucketId, -1, key);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    time += timeIncrement;
  }

  @Min(1)
  @Override
  public long getNumBuckets()
  {
    return numBuckets;
  }

  /**
   * Sets the number of buckets.
   *
   * @param numBuckets number of buckets
   */
  public void setNumBuckets(int numBuckets)
  {
    this.numBuckets = numBuckets;
  }
}
