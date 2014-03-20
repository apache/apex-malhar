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
public class TimeBasedBucketManagerImpl<T extends TimeEvent> extends BucketManagerImpl<T>
{
  @Min(1)
  protected long bucketSpanInMillis;
  @Min(0)
  protected long startOfBucketsInMillis;
  private AtomicLong expiryTime;

  private transient AtomicLong endOBucketsInMillis;
  private transient Timer bucketSlidingTimer;

  TimeBasedBucketManagerImpl()
  {
    super();
  }

  protected TimeBasedBucketManagerImpl(Builder<T> builder)
  {
    super(builder);
    this.bucketSpanInMillis = builder.bucketSpanInMillis;
    this.startOfBucketsInMillis = builder.startOfBucketsInMillis;
    this.expiryTime = new AtomicLong();
    expiryTime.set(startOfBucketsInMillis);
  }

  @Override
  public TimeBasedBucketManagerImpl<T> cloneWithProperties()
  {
    TimeBasedBucketManagerImpl<T> clone = this.newBuilder().build();
    clone.committedWindow = committedWindow;
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

  public static class Builder<T extends TimeEvent> extends BucketManagerImpl.Builder<T>
  {
    protected long bucketSpanInMillis;
    private long startOfBucketsInMillis;

    /**
     * Creates a TimeBasedBucketManagerImpl builder with following default values:<br/>
     * <ul>
     * <li>
     * {@link #noOfBucketsInMemory} =  if {m@link #noOfBuckets} > {@value #DEF_NUM_BUCKETS_MEM} then {@value #DEF_NUM_BUCKETS_MEM};
     * otherwise {@link #noOfBuckets}
     * </li>
     *
     * <li>
     * {@link #maxNoOfBucketsInMemory} = {@link #noOfBucketsInMemory} * 2
     * </li>
     *
     * <li>
     * {@link #millisPreventingBucketEviction} = {@value #DEF_MILLIS_PREVENTING_EVICTION}
     * </li>
     *
     * <li>
     * {@link #bucketStore} = {@link HdfsBucketStore}
     * </li>
     * </ul>
     *
     * @param writeEventKeysOnly true for keeping only event keys in memory and store; false otherwise.
     * @param numberOfDays       number of past days for which events are processed.
     * @param bucketSpanInMillis number of milliseconds a bucket spans.
     */
    public Builder(boolean writeEventKeysOnly, int numberOfDays, long bucketSpanInMillis)
    {
      super(writeEventKeysOnly);
      this.bucketSpanInMillis = bucketSpanInMillis;

      Calendar calendar = Calendar.getInstance();
      long now = calendar.getTimeInMillis();

      calendar.add(Calendar.DATE, -numberOfDays);
      this.startOfBucketsInMillis = calendar.getTimeInMillis();

      this.noOfBuckets = (int) Math.ceil((now - startOfBucketsInMillis) / (bucketSpanInMillis * 1.0));
      this.bucketStore = new HdfsBucketStore<T>(noOfBuckets, writeEventKeysOnly);
      this.noOfBucketsInMemory = noOfBuckets > DEF_NUM_BUCKETS_MEM ? DEF_NUM_BUCKETS_MEM : noOfBuckets;
      this.maxNoOfBucketsInMemory = noOfBucketsInMemory * 2;
      this.millisPreventingBucketEviction = DEF_MILLIS_PREVENTING_EVICTION;
    }

    private Builder(boolean writeEventKeysOnly, int noOfBuckets, long startOfBucketsInMillis, long bucketSpanInMillis)
    {
      super(writeEventKeysOnly, noOfBuckets);
      this.bucketSpanInMillis = bucketSpanInMillis;
      this.startOfBucketsInMillis = startOfBucketsInMillis;
    }

    @Override
    public TimeBasedBucketManagerImpl<T> build()
    {
      return new TimeBasedBucketManagerImpl<T>(this);
    }
  }

  @Override
  protected Builder<T> newBuilder()
  {
    return (Builder<T>) new Builder<T>(writeEventKeysOnly, noOfBuckets, startOfBucketsInMillis, bucketSpanInMillis)
      .noOfBucketsInMemory(noOfBucketsInMemory)
      .maxNoOfBucketsInMemory(maxNoOfBucketsInMemory)
      .millisPreventingBucketEviction(millisPreventingBucketEviction)
      .bucketStore(bucketStore);
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
