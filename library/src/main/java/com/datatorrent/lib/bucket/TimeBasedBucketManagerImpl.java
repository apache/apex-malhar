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

import java.io.IOException;
import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BucketManager} that creates buckets based on time.<br/>
 *
 * @param <T> event type
 * @since 0.9.4
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
  private long expiryTime;
  private Long[] maxTimesPerBuckets;

  private transient long endOBucketsInMillis;
  private transient Timer bucketSlidingTimer;
  private final transient Lock lock;

  public TimeBasedBucketManagerImpl()
  {
    super();
    daysSpan = DEF_DAYS_SPAN;
    bucketSpanInMillis = DEF_BUCKET_SPAN_MILLIS;
    lock = new Lock();
  }

  /**
   * Number of past days for which events are processed.
   *
   * @param daysSpan
   */
  public void setDaysSpan(int daysSpan)
  {
    this.daysSpan = daysSpan;
    initialize();
  }

  /**
   * Sets the number of milliseconds a bucket spans.
   *
   * @param bucketSpanInMillis
   */
  public void setBucketSpanInMillis(long bucketSpanInMillis)
  {
    this.bucketSpanInMillis = bucketSpanInMillis;
    initialize();
  }

  @Override
  public TimeBasedBucketManagerImpl<T> cloneWithProperties()
  {
    TimeBasedBucketManagerImpl<T> clone = new TimeBasedBucketManagerImpl<T>();
    copyPropertiesTo(clone);
    clone.bucketSpanInMillis = bucketSpanInMillis;
    clone.startOfBucketsInMillis = startOfBucketsInMillis;
    clone.expiryTime = expiryTime;
    clone.maxTimesPerBuckets = maxTimesPerBuckets;
    return clone;
  }

  @Override
  public void initialize()
  {
    Calendar calendar = Calendar.getInstance();
    long now = calendar.getTimeInMillis();

    calendar.add(Calendar.DATE, -daysSpan);
    startOfBucketsInMillis = calendar.getTimeInMillis();
    expiryTime = startOfBucketsInMillis;
    noOfBuckets = (int) Math.ceil((now - startOfBucketsInMillis) / (bucketSpanInMillis * 1.0));
    if (bucketStore == null) {
      bucketStore = new ExpirableHdfsBucketStore<T>();
    }
    bucketStore.setNoOfBuckets(noOfBuckets);
    bucketStore.setWriteEventKeysOnly(writeEventKeysOnly);
    maxTimesPerBuckets = new Long[noOfBuckets];
  }

  @Override
  public void startService(Context context, Listener<T> listener)
  {
    bucketSlidingTimer = new Timer();
    endOBucketsInMillis = expiryTime + (noOfBuckets * bucketSpanInMillis);
    logger.debug("bucket properties {}, {}", daysSpan, bucketSpanInMillis);
    logger.debug("bucket time params: start {}, expiry {}, end {}", startOfBucketsInMillis, expiryTime, endOBucketsInMillis);

    bucketSlidingTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        long time;
        synchronized (lock) {
          time = (expiryTime += bucketSpanInMillis);
          endOBucketsInMillis += bucketSpanInMillis;
          if (bucketCounters != null) {
            bucketCounters.high = endOBucketsInMillis;
            bucketCounters.low = expiryTime;
          }
        }
        try {
          ((BucketStore.ExpirableBucketStore<T>) bucketStore).deleteExpiredBuckets(time);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

    }, bucketSpanInMillis, bucketSpanInMillis);
    super.startService(context, listener);
  }

  @Override
  public long getBucketKeyFor(T event)
  {
    long eventTime = event.getTime();
    if (eventTime < expiryTime) {
      if (bucketCounters != null) {
        bucketCounters.numIgnoredEvents++;
      }
      return -1;
    }
    long diffFromStart = event.getTime() - startOfBucketsInMillis;
    long key = diffFromStart / bucketSpanInMillis;
    synchronized (lock) {
      if (eventTime > endOBucketsInMillis) {
        long move = ((eventTime - endOBucketsInMillis) / bucketSpanInMillis + 1) * bucketSpanInMillis;
        expiryTime += move;
        endOBucketsInMillis += move;
        if (bucketCounters != null) {
          bucketCounters.high = endOBucketsInMillis;
          bucketCounters.low = expiryTime;
        }
      }
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
    return expiryTime == that.expiryTime;

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (int) (bucketSpanInMillis ^ (bucketSpanInMillis >>> 32));
    result = 31 * result + (int) (startOfBucketsInMillis ^ (startOfBucketsInMillis >>> 32));
    result = 31 * result + (int) (expiryTime ^ (expiryTime >>> 32));
    return result;
  }

  @Override
  public void newEvent(long bucketKey, T event)
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);

    Bucket<T> bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
      bucket = new Bucket<T>(bucketKey);
      buckets[bucketIdx] = bucket;
      dirtyBuckets.put(bucketIdx, bucket);
    }
    else if (dirtyBuckets.get(bucketIdx) == null) {
      dirtyBuckets.put(bucketIdx, bucket);
    }

    bucket.addNewEvent(event.getEventKey(), writeEventKeysOnly ? null : event);
    if (bucketCounters != null) {
      synchronized (bucketCounters) {
        bucketCounters.numEventsInMemory++;
      }
    }

    Long max = maxTimesPerBuckets[bucketIdx];
    if (max == null || event.getTime() > max) {
      maxTimesPerBuckets[bucketIdx] = event.getTime();
    }
  }

  @Override
  public void endWindow(long window)
  {
    long maxTime = -1;
    for (int bucketIdx : dirtyBuckets.keySet()) {
      if (maxTimesPerBuckets[bucketIdx] > maxTime) {
        maxTime = maxTimesPerBuckets[bucketIdx];
      }
      maxTimesPerBuckets[bucketIdx] = null;
    }
    if (maxTime > -1) {
      saveData(window, maxTime);
    }
  }

  private static class Lock
  {
  }

  private static transient final Logger logger = LoggerFactory.getLogger(TimeBasedBucketManagerImpl.class);

}
