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
 *   The core class for managing buckets is {@link com.datatorrent.lib.bucket.BucketManager}. Only it can create
 *   new {@link com.datatorrent.lib.bucket.Bucket}s.<br/>
 *   BucketManager interacts with a {@link com.datatorrent.lib.bucket.BucketStore} which is provided
 *   by the user which in this case is an {@link com.datatorrent.api.Operator}. <br/>
 *   {@link com.datatorrent.lib.bucket.HdfsBucketStore} is an example of BucketStore which uses Hdfs as the persistent
 *   store.
 * </p>
 *
 * <p>
 *   There is also a {@link com.datatorrent.lib.bucket.BucketManagerForExpirable} which works with events that
 *   can be expired. It can be configured to delete these events from memory as well as persistent store periodically.
 * </p>
 *
 * <p>
 *   Usage example:
 *   {@link com.datatorrent.lib.dedup.Deduper}
 * </p>
 */
package com.datatorrent.lib.bucket;