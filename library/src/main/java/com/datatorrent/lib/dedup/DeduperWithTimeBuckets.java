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
package com.datatorrent.lib.dedup;

import java.util.Calendar;
import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;

import com.datatorrent.lib.bucket.BucketManager;
import com.datatorrent.lib.bucket.TimeEvent;

/**
 * <p>
 * A {@link Deduper} which works with events that have their bucket keys derived from the event timestamp.
 * </p>
 *
 * <p>
 * This deduper creates time based buckets. The number of buckets that are created is based on the following configurations:
 * <ol>
 * <li>{@link #bucketSpanInMillis}: span of a bucket in milliseconds. Default value is 1 minute.</li>
 * <li>{@link #startOfBucketsInMillis}: the start time of the first bucket in millis. Default value is set to 2
 * days before the operator creation time.</li>
 * </ol>
 * </p>
 *
 * <p>
 * It is based on the assumption that duplicate events fall into same time bucket.
 * </p>
 *
 * @param <INPUT>  type of input tuple</INPUT>
 * @param <OUTPUT> type of output tuple</OUTPUT>
 */
public abstract class DeduperWithTimeBuckets<INPUT extends TimeEvent, OUTPUT> extends Deduper<INPUT, OUTPUT>
{

  @Min(1)
  protected long bucketSpanInMillis;
  @Min(0)
  private long startOfBucketsInMillis;
  private long expiryTime;

  private final transient AtomicBoolean doBucketSliding;
  private transient Timer bucketSlidingTimer;

  public DeduperWithTimeBuckets()
  {
    super();
    doBucketSliding = new AtomicBoolean(false);
    bucketSpanInMillis = 60 * 1000;
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, -2);
    startOfBucketsInMillis = calendar.getTimeInMillis();
  }

  /**
   * Time buckets are created based on the span of a bucket.<br/>
   * This sets the length of a bucket. By default it is 1 minute.
   *
   * @param millis number of milliseconds a bucket spans
   */
  public void setBucketSpanInMillis(long millis)
  {
    this.bucketSpanInMillis = millis;
  }

  public long getBucketSpanInMillis()
  {
    return this.bucketSpanInMillis;
  }

  /**
   * Sets the start time of the first time bucket.<br/>
   * The different between the event time & this start time controls which bucket an event belongs to.<br/>
   * It also controls the total no. of buckets managed by {@link BucketManager}. <br/>
   * Default value is set to 2 days before the operator creation time.
   *
   * @param millis buckets start time;
   */
  public void setStartOfBucketsInMillis(long millis)
  {
    this.startOfBucketsInMillis = millis;
    expiryTime = startOfBucketsInMillis;
  }

  public long getStartOfBucketsInMillis()
  {
    return this.startOfBucketsInMillis;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    bucketSlidingTimer = new Timer();
    bucketSlidingTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        doBucketSliding.compareAndSet(false, true);
      }

    }, bucketSpanInMillis, bucketSpanInMillis);

    logger.debug("bucket start point {}", startOfBucketsInMillis);
    logger.debug("expiry time {}", expiryTime);
  }

  @Override
  protected long getEventBucketKey(INPUT event)
  {
    if (event.getTime() < expiryTime) {
      return -1;
    }
    long diffFromStart = event.getTime() - startOfBucketsInMillis;
    return diffFromStart / bucketSpanInMillis;
  }

  @Override
  public void handleIdleTime()
  {
    super.handleIdleTime();
    if (doBucketSliding.compareAndSet(true, false)) {
      expiryTime += bucketSpanInMillis;
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    bucketSlidingTimer.cancel();
  }

  @Override
  public void definePartitions(Collection<Partition<Deduper<INPUT, OUTPUT>>> oldPartitions, Collection<Partition<Deduper<INPUT, OUTPUT>>> newPartitions)
  {
    DeduperWithTimeBuckets<INPUT, OUTPUT> oldOne = (DeduperWithTimeBuckets<INPUT, OUTPUT>) oldPartitions.iterator().next().getPartitionedInstance();

    for (Partition<Deduper<INPUT, OUTPUT>> partition : newPartitions) {
      DeduperWithTimeBuckets<INPUT, OUTPUT> newDeduper = (DeduperWithTimeBuckets<INPUT, OUTPUT>) partition.getPartitionedInstance();
      newDeduper.bucketSpanInMillis = oldOne.bucketSpanInMillis;
      newDeduper.startOfBucketsInMillis = oldOne.startOfBucketsInMillis;
      newDeduper.expiryTime = oldOne.expiryTime;
    }
  }

  @Override
  public abstract OUTPUT convert(INPUT input);

  private final static Logger logger = LoggerFactory.getLogger(Deduper.class);
}
