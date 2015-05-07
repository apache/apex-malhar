/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

import com.datatorrent.lib.bucket.BucketPOJOImpl;
import com.datatorrent.lib.bucket.AbstractTimeBasedBucketManager;

/**
 * A {@link BucketManager} that creates buckets based on time.<br/>
 *
 * @since 0.9.4
 */
public class TimeBasedBucketManagerSimpleEventImpl extends AbstractTimeBasedBucketManager<SimpleEvent>
{
  @Override
  protected BucketSimpleEventImpl createBucket(long bucketKey)
  {
    return new BucketSimpleEventImpl(bucketKey);
  }

  @Override
  protected long getTime(SimpleEvent event)
  {
    return Long.parseLong(event.getHhmm());
  }
}
