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

import javax.validation.constraints.NotNull;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.state.spillable.WindowListener;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;

/**
 * Abstract class to extract a bucket for a given time
 *
 * @since 3.7.0
 */
public abstract class TimeBucketAssigner implements ManagedStateComponent, WindowListener
{
  private transient PurgeListener purgeListener;

  private boolean initialized;

  @FieldSerializer.Bind(JavaSerializer.class)
  private Duration bucketSpan;

  @Override
  public void setup(@NotNull ManagedStateContext managedStateContext)
  {
    Context.OperatorContext context = managedStateContext.getOperatorContext();
    if (!initialized && bucketSpan == null) {
      setBucketSpan(Duration.millis(context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
          context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS)));
    }
  }

  /**
   * Get the time bucket for any given time
   * @param time
   * @return
   */
  public abstract long getTimeBucket(long time);

  /**
   * Get possible number of buckets
   * @return
   */
  public abstract long getNumBuckets();

  /**
   * @return time-bucket span
   */
  public Duration getBucketSpan()
  {
    return bucketSpan;
  }

  /**
   * Sets the length of a time bucket.
   * @param bucketSpan length of time bucket
   */
  public void setBucketSpan(Duration bucketSpan)
  {
    this.bucketSpan = bucketSpan;
  }

  public boolean isInitialized()
  {
    return initialized;
  }

  public void setInitialized(boolean initialized)
  {
    this.initialized = initialized;
  }

  /**
   * Sets the purge listener.
   * @param purgeListener purge listener
   */
  public void setPurgeListener(@NotNull PurgeListener purgeListener)
  {
    this.purgeListener = Preconditions.checkNotNull(purgeListener, "purge listener");
  }

  public PurgeListener getPurgeListener()
  {
    return purgeListener;
  }

  /**
   * The listener is informed when the time slides and time buckets which are older than the smallest time bucket
   * (changed because of time slide) can be purged.
   */
  public interface PurgeListener
  {
    void purgeTimeBucketsLessThanEqualTo(long timeBucket);
  }
}
