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

/**
 * A {@link BucketStore} for {@link TimeEvent} which also supports deleting
 * expired events.
 */
public interface BucketStoreForExpirable<T extends TimeEvent> extends BucketStore<T>
{
  /**
   * Deletes events which have timestamp less than the given time from the store.
   *
   * @param earliestValidTimeInMillis time in milliseconds; all the events with timestamp less than this are deleted
   *                                  from store.
   * @throws Exception
   */
  void cleanUpExpiredEvents(long earliestValidTimeInMillis) throws Exception;
}
