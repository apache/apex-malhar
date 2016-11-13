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
 * A state where keys are grouped in buckets.
 *
 * @since 3.4.0
 */
public interface BucketedState
{
  /**
   * An expired value. In some implementations where bucketId is time related then the event can be old and
   * the get methods- getSync & getAsync return this fixed slice instance.<br/>
   * In the usages, comparisons with EXPIRED should be made using <code>==</code> instead of <code>equals</code>.
   */
  Slice EXPIRED = new Slice(null, -1, -1);

  /**
   * Sets the value of the key in bucket identified by bucketId.
   *
   * @param bucketId identifier of the bucket.
   * @param key      key (not null)
   * @param value    value (not null)
   */
  void put(long bucketId, @NotNull Slice key, @NotNull Slice value);

  /**
   * Returns the value of the key in a bucket identified by bucketId. Fetching a key can be expensive if the key
   * is not in memory and is present on disk. This fetches the key synchronously. <br/>
   * {@link #getAsync(long, Slice)} is recommended for efficiently reading the value of a key.
   *
   * @param bucketId identifier of the bucket.
   * @param key     key (not null)
   *
   * @return value of the key if found; null if the key is not found;
   * {@link #EXPIRED} if the bucketId is time based and very old.
   */
  Slice getSync(long bucketId, @NotNull Slice key);

  /**
   * Returns the future using which the value is obtained.<br/>
   *
   * @param key key (not null)
   *
   * @return value of the key if found; null if the key is not found;
   * {@link #EXPIRED} if the bucketId is time based and very old.
   */
  Future<Slice> getAsync(long bucketId, @NotNull Slice key);

}
