/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.bucket;

import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BucketManager} that creates buckets based on time.<br/>
 *
 * @param <T> event type
 */
public class TimeBasedBucketManagerImpl<T extends Event & Bucketable> extends BucketManagerImpl<T>
{
  public static int DEF_DAYS_SPAN = 2;
  public static long DEF_BUCKET_SPAN_MILLIS = 60000;

  private int daysSpan;
  @Min(1)
  protected long bucketSpanInMillis;
  @Min(0)
  protected long startOfBucketsInMillis;
  private AtomicLong expiryTime;

  private transient AtomicLong endOBucketsInMillis;
  private transient Timer bucketSlidingTimer;

  public TimeBasedBucketManagerImpl()
  {
    super();
    daysSpan = DEF_DAYS_SPAN;
    bucketSpanInMillis = DEF_BUCKET_SPAN_MILLIS;
    expiryTime = new AtomicLong();
    recomputeNumberOfBuckets();
  }

  /**
   * Number of past days for which events are processed.
   *
   * @param daysSpan
   */
  public void setDaysSpan(int daysSpan)
  {
    this.daysSpan = daysSpan;
    recomputeNumberOfBuckets();
  }

  /**
   * Sets the number of milliseconds a bucket spans.
   *
   * @param bucketSpanInMillis
   */
  public void setBucketSpanInMillis(long bucketSpanInMillis)
  {
    this.bucketSpanInMillis = bucketSpanInMillis;
    recomputeNumberOfBuckets();
  }

  private void recomputeNumberOfBuckets()
  {
    Calendar calendar = Calendar.getInstance();
    long now = calendar.getTimeInMillis();

    calendar.add(Calendar.DATE, -daysSpan);
    startOfBucketsInMillis = calendar.getTimeInMillis();
    expiryTime.set(startOfBucketsInMillis);
    noOfBuckets = (int) Math.ceil((now - startOfBucketsInMillis) / (bucketSpanInMillis * 1.0));
    bucketStore.setNoOfBuckets(noOfBuckets);
  }

  @Override
  public TimeBasedBucketManagerImpl<T> cloneWithProperties()
  {
    TimeBasedBucketManagerImpl<T> clone = new TimeBasedBucketManagerImpl<T>();
    copyPropertiesTo(clone);
    clone.bucketSpanInMillis = bucketSpanInMillis;
    clone.startOfBucketsInMillis = startOfBucketsInMillis;
    clone.expiryTime = expiryTime;
    return clone;
  }

  @Override
  public void startService(Context context, Listener<T> listener)
  {
    bucketSlidingTimer = new Timer();
    endOBucketsInMillis = new AtomicLong();
    endOBucketsInMillis.set(startOfBucketsInMillis + noOfBuckets * bucketSpanInMillis);
    logger.debug("bucket properties {}, {}", daysSpan, bucketSpanInMillis);
    logger.debug("bucket time params: start {}, expiry {}, end {}", startOfBucketsInMillis, expiryTime, endOBucketsInMillis);

    bucketSlidingTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        expiryTime.addAndGet(bucketSpanInMillis);
      }

    }, bucketSpanInMillis, bucketSpanInMillis);

    super.startService(context, listener);
  }

  @Override
  public long getBucketKeyFor(T event)
  {
    long eventTime = event.getTime();
    if (eventTime < expiryTime.get()) {
      return -1;
    }
    long diffFromStart = event.getTime() - startOfBucketsInMillis;
    long key = diffFromStart / bucketSpanInMillis;
    if (eventTime > endOBucketsInMillis.get()) {
      long move = ((eventTime - endOBucketsInMillis.get()) / bucketSpanInMillis + 1) * bucketSpanInMillis;
      expiryTime.addAndGet(move);
      endOBucketsInMillis.addAndGet(move);
    }
    return key;
  }

  @Override
  public void shutdownService()
  {
    bucketSlidingTimer.cancel();
    super.shutdownService();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimeBasedBucketManagerImpl)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    TimeBasedBucketManagerImpl that = (TimeBasedBucketManagerImpl) o;

    if (bucketSpanInMillis != that.bucketSpanInMillis) {
      return false;
    }
    if (startOfBucketsInMillis != that.startOfBucketsInMillis) {
      return false;
    }
    if (expiryTime != null ? (that.expiryTime == null || expiryTime.get() != that.expiryTime.get()) : that.expiryTime != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (int) (bucketSpanInMillis ^ (bucketSpanInMillis >>> 32));
    result = 31 * result + (int) (startOfBucketsInMillis ^ (startOfBucketsInMillis >>> 32));
    result = 31 * result + (expiryTime != null ? expiryTime.hashCode() : 0);
    return result;
  }

  private static transient final Logger logger = LoggerFactory.getLogger(TimeBasedBucketManagerImpl.class);

}
