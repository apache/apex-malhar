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

import javax.annotation.Nonnull;
import javax.validation.constraints.Min;

import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.lib.counters.BasicCounters;

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
    recomputeNumBuckets();
  }

  /**
   * Sets the number of milliseconds a bucket spans.
   *
   * @param bucketSpanInMillis
   */
  public void setBucketSpanInMillis(long bucketSpanInMillis)
  {
    this.bucketSpanInMillis = bucketSpanInMillis;
    recomputeNumBuckets();
  }

  /*
   * Gets the maximum Times Per Buckets.
   */
  public Long[] getMaxTimesPerBuckets()
  {
    return maxTimesPerBuckets;
  }

  @Override
  public TimeBasedBucketManagerImpl<T> clone() throws CloneNotSupportedException
  {
    @SuppressWarnings("unchecked")
    TimeBasedBucketManagerImpl<T> clone = (TimeBasedBucketManagerImpl<T>)super.clone();
    clone.maxTimesPerBuckets = maxTimesPerBuckets.clone();
    return clone;
  }

  @Override
  public void setBucketStore(@Nonnull BucketStore<T> store)
  {
    Preconditions.checkArgument(store instanceof BucketStore.ExpirableBucketStore);
    this.bucketStore = store;
    recomputeNumBuckets();
  }

  private void recomputeNumBuckets()
  {
    Calendar calendar = Calendar.getInstance();
    long now = calendar.getTimeInMillis();
    calendar.add(Calendar.DATE, -daysSpan);
    startOfBucketsInMillis = calendar.getTimeInMillis();
    expiryTime = startOfBucketsInMillis;
    noOfBuckets = (int) Math.ceil((now - startOfBucketsInMillis) / (bucketSpanInMillis * 1.0));
    if (bucketStore != null) {
      bucketStore.setNoOfBuckets(noOfBuckets);
      bucketStore.setWriteEventKeysOnly(writeEventKeysOnly);
    }
    maxTimesPerBuckets = new Long[noOfBuckets];
  }

  @Override
  public void setBucketCounters(@Nonnull BasicCounters<MutableLong> bucketCounters)
  {
    super.setBucketCounters(bucketCounters);
    bucketCounters.setCounter(CounterKeys.LOW, new MutableLong());
    bucketCounters.setCounter(CounterKeys.HIGH, new MutableLong());
    bucketCounters.setCounter(CounterKeys.IGNORED_EVENTS, new MutableLong());
  }

  @Override
  public void startService(Listener<T> listener)
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
          if (recordStats) {
            bucketCounters.getCounter(CounterKeys.HIGH).setValue(endOBucketsInMillis);
            bucketCounters.getCounter(CounterKeys.LOW).setValue(expiryTime);
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
    super.startService(listener);
  }

  @Override
  public long getBucketKeyFor(T event)
  {
    long eventTime = event.getTime();
    if (eventTime < expiryTime) {
      if (recordStats) {
        bucketCounters.getCounter(CounterKeys.IGNORED_EVENTS).increment();
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
        if (recordStats) {
          bucketCounters.getCounter(CounterKeys.HIGH).setValue(endOBucketsInMillis);
          bucketCounters.getCounter(CounterKeys.LOW).setValue(expiryTime);
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

    @SuppressWarnings("unchecked")
    TimeBasedBucketManagerImpl<T> that = (TimeBasedBucketManagerImpl<T>) o;

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
    bucketCounters.getCounter(BucketManager.CounterKeys.EVENTS_IN_MEMORY).increment();

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

  public static enum CounterKeys
  {
    LOW, HIGH, IGNORED_EVENTS
  }

  private static transient final Logger logger = LoggerFactory.getLogger(TimeBasedBucketManagerImpl.class);

}
