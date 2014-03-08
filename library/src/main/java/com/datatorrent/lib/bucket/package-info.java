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

/**
 * This package contains the bucketing utility which can be used by an operator to optimize the use
 * of memory and minimize the latency of reading/writing data to disk.<br/>
 *
 * <p>
 *   The api of a bucket manager is provided by {@link com.datatorrent.lib.bucket.BucketManager}.<br/>
 *   There are 2 concrete implementations:
 *   <ul>
 *     <li>{@link com.datatorrent.lib.bucket.BucketManagerImpl}</li>
 *     <li>{@link com.datatorrent.lib.bucket.TimeBasedBucketManagerImpl}</li>
 *   </ul>
 *   Only a BucketManager can create new {@link com.datatorrent.lib.bucket.Bucket}s. It interacts with a {@link com.datatorrent.lib.bucket.BucketStore} which is provided
 *   by the user which in this case is an {@link com.datatorrent.api.Operator}. <br/>
 *   {@link com.datatorrent.lib.bucket.HdfsBucketStore} is an example of BucketStore which uses Hdfs as the persistent
 *   store.
 * </p>
 *
 * <p>
 *   Usage example:
 *   {@link com.datatorrent.lib.dedup.Deduper}
 * </p>
 */
package com.datatorrent.lib.bucket;