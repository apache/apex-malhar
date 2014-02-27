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
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Bucket store API.<br/>
 */
public interface BucketStore<T extends BucketEvent>
{
  /**
   * Performs setup operations eg. crate database connections, delete events of windows greater than last committed
   * window, etc.
   *
   * @param noOfBuckets              total number of buckets.
   * @param committedWindowOfLastRun the last committed window; events of windows after the would be deleted from the
   *                                 store.
   * @param writeEventKeysOnly       write only event keys not the complete events.
   * @throws Exception
   */
  void setup(int noOfBuckets, long committedWindowOfLastRun, boolean writeEventKeysOnly) throws Exception;

  /**
   * Performs teardown operations eg. close database connections.
   */
  void teardown();

  /**
   * Stores the un-written bucket data collected in the given window.
   *
   * @param bucketIdx  index of bucket.
   * @param window     window in which data was collected.
   * @param bucketData bucket events to be persisted.
   */
  void storeBucketData(int bucketIdx, long window, Map<Object, T> bucketData) throws Exception;

  /**
   * Deletes bucket corresponding to the bucket index from the persistent store.
   *
   * @param bucketIdx index of the bucket to delete.
   */
  void deleteBucket(int bucketIdx) throws IOException;

  /**
   * Fetches events of the bucket corresponding to the bucket index from the store.
   *
   * @param bucketIdx index of bucket.
   * @return bucket events
   * @throws Exception
   */
  @Nonnull
  Map<Object, T> fetchBucket(int bucketIdx) throws Exception;
}