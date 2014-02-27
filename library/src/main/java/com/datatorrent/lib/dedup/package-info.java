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
 * This package contains operators which drop duplicate events. They group events in buckets and use
 * {@link com.datatorrent.lib.bucket.BucketManager} for better memory management.
 *
 * <p>
 *  <ul>
 *   <li>{@link com.datatorrent.lib.dedup.Deduper}: base de-duplication operator.</li>
 *   <li>{@link com.datatorrent.lib.dedup.DeduperWithTimeBuckets}: this operator works with events which can be grouped
 *   in buckets according to their time.</li>
 *   <li>{@link com.datatorrent.lib.dedup.HDFSBasedDeduper}: a {@link com.datatorrent.lib.dedup.DeduperWithTimeBuckets}
 *    that uses {@link com.datatorrent.lib.bucket.HdfsBucketStore}.</li>
 *   </ul>
 * </p>
 *
 */
package com.datatorrent.lib.dedup;