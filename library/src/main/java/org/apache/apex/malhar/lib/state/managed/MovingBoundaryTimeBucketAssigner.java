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
import org.joda.time.Instant;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

/**
 * Keeps track of time buckets and triggers purging of obsolete time-buckets that moved out of boundary.<br/>
 *
 * The data of a bucket is further divided into time-buckets. This component controls the length of time buckets,
 * which time-bucket an event falls into and sliding the time boundaries.
 * <p/>
 *
 * The configuration {@link #expireBefore}, {@link #bucketSpan} and {@link #referenceInstant} (default time: system
 * time during initialization of TimeBucketAssigner) are used to calculate number of time-buckets.<br/>
 * For eg. if <code>expireBefore = 1 hour</code>, <code>bucketSpan = 30 minutes</code> and
 * <code>rererenceInstant = currentTime</code>, then <code>
 *   numBuckets = 60 minutes/ 30 minutes = 2 </code>.<br/>
 *
 * These properties once configured shouldn't be changed because that will result in different time-buckets
 * for the same (key,time) pair after a failure.
 * <p/>
 *
 * The time boundaries- start and end, move by multiples of time-bucket span. Any event with time < start
 * is considered expired. The boundaries slide by {@link #getTimeBucket(long)}. The time which is passed as an
 * argument to this method can be ahead of <code>end</code>. This means that the corresponding event is a future event
 * (wrt TimeBucketAssigner) and cannot be ignored. Therefore it is accounted by sliding boundaries further.
 *
 *
 * @since 3.7.0
 */
public class MovingBoundaryTimeBucketAssigner extends TimeBucketAssigner
{
  private long start;

  private long end;

  @NotNull
  private Instant referenceInstant = new Instant();

  @NotNull
  @FieldSerializer.Bind(JavaSerializer.class)
  private Duration expireBefore = Duration.standardDays(2);

  private long bucketSpanMillis;

  private int numBuckets;
  private transient long fixedStart;
  private transient boolean triggerPurge;
  private transient long lowestPurgeableTimeBucket = -1;


  @Override
  public void setup(@NotNull ManagedStateContext managedStateContext)
  {
    super.setup(managedStateContext);
    fixedStart = referenceInstant.getMillis() - expireBefore.getMillis();

    if (!isInitialized()) {

      start = fixedStart;
      bucketSpanMillis = getBucketSpan().getMillis();
      numBuckets = (int)((expireBefore.getMillis() + bucketSpanMillis - 1) / bucketSpanMillis);
      end = start + (numBuckets * bucketSpanMillis);

      setInitialized(true);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    if (triggerPurge && getPurgeListener() != null) {
      triggerPurge = false;
      getPurgeListener().purgeTimeBucketsLessThanEqualTo(lowestPurgeableTimeBucket);
    }
  }

  @Override
  public void teardown()
  {
  }

  /**
   * Get the bucket key for the long value and adjust boundaries if necessary. If boundaries adjusted then verify
   * the triggerPurge is enabled or not. triggerPurge is enabled only when the lower bound changes.
   *
   * For example,
   * ExpiryDuration = 1000 milliseconds, BucketSpan = 2000 milliseconds
   * Times with 0,...999 belongs to time bucket id 0, times with 1000,...1999 belongs to bucket id 1,...so on.
   * Initially start = 0, end = 2000, fixedStart = 0
   * (1) If the input with time 50 milliseconds then this belongs to bucket id 0.
   *
   * (2) If the input with time 2100 milliseconds then boundary has to be adjusted.
   *    Values after tuple is processed, diffInBuckets = 0, move = 1000, start = 1000, end = 3000,triggerPurge = true, lowestPurgeableTimeBucket = -1
   *
   * (3) If the input with time 3200 milliseconds then boundary has to be adjusted.
   *    Values after tuple is processed, diffInBuckets = 0, move = 1000, start = 2000, end = 4000,triggerPurge = true, lowestPurgeableTimeBucket = 0
   *
   * @param time value from which bucket key is derived.
   * @return -1 if value is already expired; bucket key otherwise.
   */
  @Override
  public long getTimeBucket(long time)
  {
    if (time < start) {
      return -1;
    }
    long diffFromStart = time - fixedStart;
    long key = diffFromStart / bucketSpanMillis;
    if (time >= end) {
      long diffInBuckets = (time - end) / bucketSpanMillis;
      long move = (diffInBuckets + 1) * bucketSpanMillis;
      start += move;
      end += move;
      // trigger purge when lower bound changes
      triggerPurge = (move > 0);
      if (triggerPurge) {
        lowestPurgeableTimeBucket = ((start - fixedStart) / bucketSpanMillis) - 2;
      }
    }
    return key;

  }

  /**
   * @return number of buckets.
   */
  @Override
  public long getNumBuckets()
  {
    return numBuckets;
  }

  /**
   * @return reference instant
   */
  public Instant getReferenceInstant()
  {
    return referenceInstant;
  }

  /**
   * Sets the reference instant (by default the system time when the streaming app is created).
   * This instant with {@link #expireBefore} is used to calculate the {@link #start} and {@link #end}.
   *
   * @param referenceInstant
   */
  public void setReferenceInstant(Instant referenceInstant)
  {
    this.referenceInstant = referenceInstant;
  }

  /**
   * @return duration before which the data is expired.
   */
  public Duration getExpireBefore()
  {
    return expireBefore;
  }

  /**
   * Sets the duration which denotes expiry. Any event with time before this duration is considered to be expired.
   * @param expireBefore duration
   */
  public void setExpireBefore(Duration expireBefore)
  {
    this.expireBefore = expireBefore;
  }

  public long getLowestPurgeableTimeBucket()
  {
    return lowestPurgeableTimeBucket;
  }
}
