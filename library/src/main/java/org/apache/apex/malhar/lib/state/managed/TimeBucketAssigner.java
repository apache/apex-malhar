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
import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.lib.appdata.query.WindowBoundedService;

/**
 * Keeps track of time buckets.<br/>
 *
 * The data of a bucket is further divided into time-buckets. This component controls the length of time buckets,
 * which time-bucket an event falls into and sliding the time boundaries.
 * <p/>
 *
 * The configuration {@link #expireBefore}, {@link #bucketSpan} and {@link #referenceInstant} (default time: system
 * time during initialization of TimeBucketAssigner) are used to calculate number of time-buckets.<br/>
 * For eg. if <code>expireBefore = 1 hour</code>, <code>bucketSpan = 30 minutes</code> and
 * <code>rererenceInstant = current-time</code>, then <code>
 *   numBuckets = 60 minutes/ 30 minutes = 2 </code>.<br/>
 *
 * These properties once configured shouldn't be changed because that will result in different time-buckets
 * for the same (key,time) pair after a failure.
 * <p/>
 *
 * The time boundaries- start and end, periodically move by span of a single time-bucket. Any event with time < start
 * is expired. These boundaries slide between application window by the expiry task asynchronously.<br/>
 * The boundaries move only between an application window to ensure consistency of a checkpoint. Checkpoint will happen
 * at application window boundaries so if we do not restrict moving start and end within an app window boundary, it may
 * happen that old value of 'start' is saved with the new value of 'end'.
 *
 * <p/>
 *
 * The boundaries can also be moved by {@link #getTimeBucketFor(long)}. The time which is passed as an argument to this
 * method can be ahead of <code>end</code>. This means that the corresponding event is a future event
 * (wrt TimeBucketAssigner) and cannot be ignored. Therefore it is accounted by sliding boundaries further.
 *
 * @since 3.4.0
 */
public class TimeBucketAssigner implements ManagedStateComponent
{
  @NotNull
  private Instant referenceInstant = new Instant();

  @NotNull
  @FieldSerializer.Bind(JavaSerializer.class)
  private Duration expireBefore = Duration.standardDays(2);

  @FieldSerializer.Bind(JavaSerializer.class)
  private Duration bucketSpan;

  private long bucketSpanMillis;

  private long start;
  private long end;
  private int numBuckets;
  private transient long fixedStart;
  private transient long lowestTimeBucket;

  private boolean initialized;

  private transient WindowBoundedService windowBoundedService;

  private transient PurgeListener purgeListener = null;

  private final transient Runnable expiryTask = new Runnable()
  {
    @Override
    public void run()
    {
      synchronized (lock) {
        start += bucketSpanMillis;
        end += bucketSpanMillis;
        if (purgeListener != null) {
          purgeListener.purgeTimeBucketsLessThanEqualTo(lowestTimeBucket++);
        }
      }
    }
  };

  private final transient Object lock = new Object();

  @Override
  public void setup(@NotNull ManagedStateContext managedStateContext)
  {
    Context.OperatorContext context = managedStateContext.getOperatorContext();
    fixedStart = referenceInstant.getMillis() - expireBefore.getMillis();

    if (!initialized) {
      if (bucketSpan == null) {
        bucketSpan = Duration.millis(context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT) *
            context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS));
      }
      start = fixedStart;
      bucketSpanMillis = bucketSpan.getMillis();
      numBuckets = (int)((expireBefore.getMillis() + bucketSpanMillis - 1) / bucketSpanMillis);
      end = start + (numBuckets * bucketSpanMillis);

      initialized = true;
    }
    lowestTimeBucket = (start - fixedStart) / bucketSpanMillis;
    windowBoundedService = new WindowBoundedService(bucketSpanMillis, expiryTask);
    windowBoundedService.setup(context);
  }

  public void beginWindow(long windowId)
  {
    windowBoundedService.beginWindow(windowId);
  }

  public void endWindow()
  {
    windowBoundedService.endWindow();
  }

  /**
   * Get the bucket key for the long value.
   *
   * @param value value from which bucket key is derived.
   * @return -1 if value is already expired; bucket key otherwise.
   */
  public long getTimeBucketFor(long value)
  {
    synchronized (lock) {
      if (value < start) {
        return -1;
      }
      long diffFromStart = value - fixedStart;
      long key = diffFromStart / bucketSpanMillis;
      if (value > end) {
        long move = ((value - end) / bucketSpanMillis + 1) * bucketSpanMillis;
        start += move;
        end += move;
      }
      return key;
    }
  }

  public void setPurgeListener(@NotNull PurgeListener purgeListener)
  {
    this.purgeListener = Preconditions.checkNotNull(purgeListener, "purge listener");
  }

  @Override
  public void teardown()
  {
    windowBoundedService.teardown();
  }

  /**
   * @return number of buckets.
   */
  public int getNumBuckets()
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

  /**
   * The listener is informed when the time slides and time buckets which are older than the smallest time bucket
   * (changed because of time slide) can be purged.
   */
  public interface PurgeListener
  {
    void purgeTimeBucketsLessThanEqualTo(long timeBucket);
  }

}
