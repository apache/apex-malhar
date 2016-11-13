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
package org.apache.apex.malhar.lib.state;

import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import com.datatorrent.netlet.util.Slice;

/**
 * A type of bucketed state where a bucket's data is further divided into time buckets. This requires
 * time per key to figure out which time bucket a particular key belongs to.
 * <p/>
 * The time here is mainly used for purging of aged key/value pair.
 *
 * @since 3.4.0
 */
public interface TimeSlicedBucketedState
{
  /**
   * Sets the value of a key in the bucket identified by bucketId. Time is used to derive which time bucket (within
   * the main bucket) a key belongs to.
   *
   * @param bucketId identifier of the bucket.
   * @param time    time associated with the key.
   * @param key     key   (not null)
   * @param value   value (not null)
   */
  void put(long bucketId, long time, @NotNull Slice key, @NotNull Slice value);

  /**
   * Returns the value of the key in the bucket identified by bucketId.<br/>
   * If the value of the key is not present in the bucket cache then this scans all the time bucket files on disk from
   * the latest to the oldest.
   *
   * It retrieves the value synchronously that can be expensive.<br/>
   * {@link #getAsync(long, Slice)} is recommended for efficient reading the value of a key.
   *
   *
   * @param bucketId identifier of the bucket
   * @param key key (not null)
   *
   * @return value of the key if found; null if the key is not found;
   */
  Slice getSync(long bucketId, @NotNull Slice key);


  /**
   * Returns the value of key in the bucket identified by bucketId.<br/>
   * If the value of the key is not present in the bucket cache then this will use the time to derive the time
   * bucket and just search for the key in a particular time bucket file.<br/>
   *
   * It retrieves the value synchronously which can be expensive.<br/>
   * {@link #getAsync(long, long, Slice)} is recommended for efficiently reading the value of a key.
   *
   * @param bucketId identifier of the bucket.
   * @param time  time for deriving the time bucket.
   * @param key   key (not null)
   *
   * @return value of the key if found; null if the key is not found; {@link BucketedState#EXPIRED} if the time is old.
   */
  Slice getSync(long bucketId, long time, @NotNull Slice key);

  /**
   * Returns the future using which the value is obtained.<br/>
   * If the value of the key is not present in the bucket cache then this searches for it in all the time buckets on
   * disk.<br/>
   * Time-buckets are looked-up in order from newest to oldest.
   *
   * @param bucketId identifier of the bucket.
   * @param key      key (not null)
   *
   * @return value of the key if found; null if the key is not found;
   */
  Future<Slice> getAsync(long bucketId, @NotNull Slice key);

  /**
   * Returns the future using which the value is obtained.<br/>
   * If the value of the key is not present in the bucket cache then this will use the time to derive the time
   * bucket and just search for the key in a particular time bucket file.<br/>
   *
   * @param bucketId  identifier of the bucket.
   * @param time     time associated with the key.
   * @param key      key  (not null)
   *
   * @return value of the key if found; null if the key is not found; {@link BucketedState#EXPIRED} if time is very old.
   */
  Future<Slice> getAsync(long bucketId, long time, @NotNull Slice key);
}
