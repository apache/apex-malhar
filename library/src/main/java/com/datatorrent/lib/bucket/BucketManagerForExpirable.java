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

import java.util.*;

import javax.validation.constraints.Min;

import com.datatorrent.common.util.DTThrowable;

/**
 * <p>
 * A {@link BucketManager} which periodically cleans events from memory and persistent store that are expired.
 * </p>
 * <p>
 * In addition to the properties of {@link BucketManager}, it has two more:
 * <ul>
 * <li>
 * {@link #cleanupTimerIntervalInMillis}: the interval in milliseconds at which clean-up task is performed
 * regularly. Default value of this setting is 1 minute.
 * </li>
 * <li>
 * {@link #eventValidDurationInMillis}: duration in milliseconds which is subtracted from the current time to find
 * out the earliest valid time. An event with timestamp before this would be deleted. Default value is 2 days.
 * </li>
 * </ul>
 * </p>
 * @param <T>
 */
public class BucketManagerForExpirable<T extends TimeEvent> extends BucketManager<T>
{
  private static transient final int DEFAULT_CLEANUP_INTERVAL = 60 * 1000; //1 minute
  private static transient final int DEFAULT_EVENT_VALID_DURATION = 2 * 24 * 60 * 60 * 1000; //2 days

  //Check-pointed
  @Min(500)
  private long cleanupTimerIntervalInMillis;
  @Min(0)
  private long eventValidDurationInMillis;

  //Non check-pointed
  private transient Timer cleanupTimer;

  BucketManagerForExpirable()
  {
    super();
    cleanupTimerIntervalInMillis = DEFAULT_CLEANUP_INTERVAL;
    eventValidDurationInMillis = DEFAULT_EVENT_VALID_DURATION;
  }

  /**
   * Constructs a manager with given parameters and default values of: <br/>
   * <ul>
   * <li>{@link #cleanupTimerIntervalInMillis}: 1 minute</li>
   * <li>{@link #eventValidDurationInMillis}: 2 days</li>
   * </ul>
   *
   * @param writeEventKeysOnly             true for keeping only event keys in memory and store; false otherwise.
   * @param noOfBuckets                    total no. of buckets.
   * @param noOfBucketsInMemory            number of buckets in memory.
   * @param millisPreventingBucketEviction duration in millis that prevent a bucket from being off-loaded.
   */
  public BucketManagerForExpirable(boolean writeEventKeysOnly, int noOfBuckets, int noOfBucketsInMemory,
                                   long millisPreventingBucketEviction)
  {
    super(writeEventKeysOnly, noOfBuckets, noOfBucketsInMemory, millisPreventingBucketEviction);
    cleanupTimerIntervalInMillis = DEFAULT_CLEANUP_INTERVAL;
    eventValidDurationInMillis = DEFAULT_EVENT_VALID_DURATION;
  }

  /**
   * Constructs a manager with given parameters and default values of: <br/>
   * <ul>
   * <li>{@link #cleanupTimerIntervalInMillis}: 1 minute</li>
   * <li>{@link #eventValidDurationInMillis}: 2 days</li>
   * </ul>
   *
   * @param writeEventKeysOnly             true for keeping only event keys in memory and store; false otherwise.
   * @param noOfBuckets                    total no. of buckets.
   * @param noOfBucketsInMemory            number of buckets in memory.
   * @param millisPreventingBucketEviction duration in millis that prevent a bucket from being off-loaded.
   * @param maxNoOfBucketsInMemory         hard limit on no. of buckets in memory.
   */
  public BucketManagerForExpirable(boolean writeEventKeysOnly, int noOfBuckets, int noOfBucketsInMemory,
                                   long millisPreventingBucketEviction, int maxNoOfBucketsInMemory)
  {
    super(writeEventKeysOnly, noOfBuckets, noOfBucketsInMemory, millisPreventingBucketEviction, maxNoOfBucketsInMemory);
    cleanupTimerIntervalInMillis = DEFAULT_CLEANUP_INTERVAL;
    eventValidDurationInMillis = DEFAULT_EVENT_VALID_DURATION;
  }

  /**
   * Constructs a manager with given parameters.
   *
   * @param writeEventKeysOnly             true for keeping only event keys in memory and store; false otherwise.
   * @param noOfBuckets                    total no. of buckets.
   * @param noOfBucketsInMemory            number of buckets in memory.
   * @param millisPreventingBucketEviction duration in millis that prevent a bucket from being off-loaded.
   * @param cleanupTimerIntervalInMillis   milliseconds at which the cleanup task is run regularly.
   * @param eventValidDurationInMillis     milliseconds to find the earliest valid time.
   */
  public BucketManagerForExpirable(boolean writeEventKeysOnly, int noOfBuckets, int noOfBucketsInMemory,
                                   long millisPreventingBucketEviction, long cleanupTimerIntervalInMillis,
                                   long eventValidDurationInMillis)
  {
    super(writeEventKeysOnly, noOfBuckets, noOfBucketsInMemory, millisPreventingBucketEviction);
    this.cleanupTimerIntervalInMillis = cleanupTimerIntervalInMillis;
    this.eventValidDurationInMillis = eventValidDurationInMillis;
  }

  /**
   * Constructs a manager with given parameters.
   *
   * @param writeEventKeysOnly             true for keeping only event keys in memory and store; false otherwise.
   * @param noOfBuckets                    total no. of buckets.
   * @param noOfBucketsInMemory            number of buckets in memory.
   * @param millisPreventingBucketEviction duration in millis that prevent a bucket from being off-loaded.
   * @param maxNoOfBucketsInMemory         hard limit on no. of buckets in memory.
   * @param cleanupTimerIntervalInMillis   milliseconds at which the cleanup task is run regularly.
   * @param eventValidDurationInMillis     milliseconds to find the earliest valid time.
   */
  public BucketManagerForExpirable(boolean writeEventKeysOnly, int noOfBuckets, int noOfBucketsInMemory,
                                   long millisPreventingBucketEviction, int maxNoOfBucketsInMemory,
                                   long cleanupTimerIntervalInMillis, long eventValidDurationInMillis)
  {
    super(writeEventKeysOnly, noOfBuckets, noOfBucketsInMemory, millisPreventingBucketEviction, maxNoOfBucketsInMemory);
    this.cleanupTimerIntervalInMillis = cleanupTimerIntervalInMillis;
    this.eventValidDurationInMillis = eventValidDurationInMillis;
  }

  /**
   * Starts the service.
   *
   * @param bucketStore a {@link BucketStore} to/from which data is saved/loaded.
   * @param listener    {@link Listener} which will be informed when bucket are loaded and off-loaded.
   * @throws Exception
   */
  @Override
  public void startService(final BucketStore<T> bucketStore, final Listener listener) throws Exception
  {
    super.startService(bucketStore, listener);
    cleanupTimer = new Timer();
    cleanupTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        long earliestValidTime = Calendar.getInstance().getTimeInMillis() - eventValidDurationInMillis;

        for (long bucketKey : knownBucketKeys) {
          Bucket<T> bucket = buckets[(int) (bucketKey % noOfBuckets)];

          if (bucket.getWrittenEvents() != null) {
            removeExpiredEvents(earliestValidTime, bucket.getWrittenEvents());
          }
          if (bucket.getUnwrittenEvents() != null) {
            removeExpiredEvents(earliestValidTime, bucket.getUnwrittenEvents());
          }
        }

        try {
          ((BucketStoreForExpirable<T>) bucketStore).cleanUpExpiredEvents(earliestValidTime);
        }
        catch (Throwable cause) {
          DTThrowable.rethrow(cause);
        }

      }
    }, cleanupTimerIntervalInMillis, cleanupTimerIntervalInMillis);
  }

  private void removeExpiredEvents(long earliestValidTime, Map<Object, T> bucketData)
  {
    for (Iterator<Map.Entry<Object, T>> iterator = bucketData.entrySet().iterator();
         iterator.hasNext(); ) {
      T event = iterator.next().getValue();
      if (event.getTime() < earliestValidTime) {
        iterator.remove();
      }
    }
  }

  /**
   * Shuts the service down.
   */
  @Override
  public void shutdownService()
  {
    super.shutdownService();
    cleanupTimer.cancel();
  }
}
