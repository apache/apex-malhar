/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.bucket;

import com.datatorrent.lib.counters.BasicCounters;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import javax.annotation.Nonnull;
import javax.validation.constraints.Min;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 * @param <T>
 */
public abstract class AbstractTimeBasedBucketManagerImpl<T> extends AbstractBucketManager<T>
{
  public static int DEF_DAYS_SPAN = 2;
  public static long DEF_BUCKET_SPAN_MILLIS = 60000;
  protected static final transient Logger logger = LoggerFactory.getLogger(AbstractTimeBasedBucketManagerImpl.class);
  protected int daysSpan;
  @Min(value = 1)
  protected long bucketSpanInMillis;
  @Min(value = 0)
  protected long startOfBucketsInMillis;
  protected long expiryTime;
  protected Long[] maxTimesPerBuckets;
  protected transient long endOBucketsInMillis;
  protected transient Timer bucketSlidingTimer;
  protected final transient Lock lock;

  public AbstractTimeBasedBucketManagerImpl()
  {
    super();
    daysSpan = DEF_DAYS_SPAN;
    bucketSpanInMillis = DEF_BUCKET_SPAN_MILLIS;
    lock = new Lock();
  }

  protected abstract long getTime(T event);


  private static class Lock
  {
  }

  public static enum CounterKeys
  {
    LOW, HIGH, IGNORED_EVENTS
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
   * Number of past days for which events are processed.
   *
   * @return daysSpan
   */
  public int getDaysSpan()
  {
    return daysSpan;
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

  /**
   * Sets the number of milliseconds a bucket spans.
   *
   * @return bucketSpanInMillis
   */
  public long getBucketSpanInMillis()
  {
    return bucketSpanInMillis;
  }

  @Override
  public AbstractTimeBasedBucketManagerImpl<T> cloneWithProperties()
  {
    AbstractTimeBasedBucketManagerImpl<T> clone = (AbstractTimeBasedBucketManagerImpl<T>)getBucketManagerImpl();
    copyPropertiesTo(clone);
    clone.bucketSpanInMillis = bucketSpanInMillis;
    clone.startOfBucketsInMillis = startOfBucketsInMillis;
    clone.expiryTime = expiryTime;
    clone.maxTimesPerBuckets = maxTimesPerBuckets;
    return clone;
  }

  @Override
  public void setBucketStore(@Nonnull BucketStore store)
  {
    Preconditions.checkArgument(store instanceof BucketStore.ExpirableBucketStore);
    this.bucketStore = store;
    recomputeNumBuckets();
  }

  protected void recomputeNumBuckets()
  {
    Calendar calendar = Calendar.getInstance();
    long now = calendar.getTimeInMillis();
    calendar.add(Calendar.DATE, -daysSpan);
    startOfBucketsInMillis = calendar.getTimeInMillis();
    expiryTime = startOfBucketsInMillis;
    noOfBuckets = (int)Math.ceil((now - startOfBucketsInMillis) / (bucketSpanInMillis * 1.0));
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
  public void startService(Listener listener)
  {
    bucketSlidingTimer = new Timer();
    endOBucketsInMillis = expiryTime + (noOfBuckets * bucketSpanInMillis);
    logger.debug("bucket properties {}, {}", daysSpan, bucketSpanInMillis);
    logger.debug("bucket time params: start {}, expiry {}, end {}", startOfBucketsInMillis, expiryTime, endOBucketsInMillis);
    bucketSlidingTimer.scheduleAtFixedRate(new TimerTask() {
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
          ((BucketStore.ExpirableBucketStore<HashMap<String, Object>>)bucketStore).deleteExpiredBuckets(time);
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
    long eventTime = getTime(event);
    if (eventTime < expiryTime) {
      if (recordStats) {
        bucketCounters.getCounter(CounterKeys.IGNORED_EVENTS).increment();
      }
      return -1;
    }
    long diffFromStart = eventTime - startOfBucketsInMillis;
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
  public void newEvent(long bucketKey, T event)
  {
    int bucketIdx = (int) (bucketKey % noOfBuckets);

    AbstractBucket<T> bucket = buckets[bucketIdx];

    if (bucket == null || bucket.bucketKey != bucketKey) {
      bucket = createBucket(bucketKey);
      buckets[bucketIdx] = bucket;
      dirtyBuckets.put(bucketIdx, bucket);
    }
    else if (dirtyBuckets.get(bucketIdx) == null) {
      dirtyBuckets.put(bucketIdx, bucket);
    }

    bucket.addNewEvent(getEventKey(event), writeEventKeysOnly ? null : event);
    bucketCounters.getCounter(BucketManager.CounterKeys.EVENTS_IN_MEMORY).increment();

    Long max = maxTimesPerBuckets[bucketIdx];
    long eventTime = getTime(event);
    if (max == null || eventTime > max) {
      maxTimesPerBuckets[bucketIdx] = eventTime;
    }
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
    if(!(o instanceof AbstractTimeBasedBucketManagerImpl))
      return false;

    if (!super.equals(o)) {
      return false;
    }

    @SuppressWarnings("unchecked")
    AbstractTimeBasedBucketManagerImpl<T> that = (AbstractTimeBasedBucketManagerImpl<T>)o;
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
    result = 31 * result + (int)(bucketSpanInMillis ^ (bucketSpanInMillis >>> 32));
    result = 31 * result + (int)(startOfBucketsInMillis ^ (startOfBucketsInMillis >>> 32));
    result = 31 * result + (int)(expiryTime ^ (expiryTime >>> 32));
    return result;
  }

  @Override
  public void endWindow(long window)
  {
    long maxTime = -1;
    for (int bucketIdx: dirtyBuckets.keySet()) {
      if (maxTimesPerBuckets[bucketIdx] > maxTime) {
        maxTime = maxTimesPerBuckets[bucketIdx];
      }
      maxTimesPerBuckets[bucketIdx] = null;
    }
    if (maxTime > -1) {
      saveData(window, maxTime);
    }
  }

}
