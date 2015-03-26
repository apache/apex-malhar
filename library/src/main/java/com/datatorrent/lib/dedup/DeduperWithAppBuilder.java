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

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.bucket.Bucket;
import com.datatorrent.lib.bucket.BucketManager;
import com.datatorrent.lib.bucket.Bucketable;

/**
 * This is the base implementation of a deduper, which drops duplicate events.&nbsp;
 * Subclasses must implement the convert method which turns input tuples into output tuples.
 * <p>
 * Processing of an event involves:
 * <ol>
 * <li>Finding the bucket key of an event by calling {@link BucketManager#getBucketKeyFor(Bucketable)}.</li>
 * <li>Getting the bucket from {@link BucketManager} by calling {@link BucketManager#getBucket(long)}.</li>
 * <li>
 * If the bucket is not loaded:
 * <ol>
 * <li>it requests the {@link BucketManager} to load the bucket which is a non-blocking call.</li>
 * <li>Adds the event to {@link #waitingEvents} which is a collection of events that are waiting for buckets to be loaded.</li>
 * <li>{@link BucketManager} loads the bucket and informs deduper by calling {@link #bucketLoaded(Bucket)}</li>
 * <li>The deduper then processes the waiting events in {@link #handleIdleTime()}</li>
 * </ol>
 * <li>
 * If the bucket is loaded, the operator drops the event if it is already present in the bucket; emits it otherwise.
 * </li>
 * </ol>
 * </p>
 *
 * <p>
 * Based on the assumption that duplicate events fall in the same bucket.
 * </p>
 *
 * @displayName Deduper
 * @category Deduplication
 * @tags dedupe
 *
 * @param <INPUT>  type of input tuple
 * @param <OUTPUT> type of output tuple
 * @since 0.9.4
 */
public class DeduperWithAppBuilder extends Deduper<HashMap<String,Object>, HashMap<String,Object>>
{
  private final static Logger logger = LoggerFactory.getLogger(DeduperWithAppBuilder.class);

  @Override
  protected HashMap<String, Object> convert(HashMap<String, Object> input)
  {
    return input;
  }

  @Override
  protected int getPartitionKey(HashMap<String,Object> event, int mask)
  {
     int partition = event.get(customKey.getEventKey()).hashCode() & mask;
     return partition;
  }


}