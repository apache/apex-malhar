/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.appdata.schemas;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public enum TimeBucket
{
  //DO NOT change order of enums. Ordinal is used as id for storage.
  ALL("all", null),
  SECOND("1s", TimeUnit.SECONDS),
  MINUTE("1m", TimeUnit.MINUTES),
  HOUR("1h", TimeUnit.HOURS),
  DAY("1d", TimeUnit.DAYS);

  public static final Map<String, TimeBucket> BUCKET_TO_TYPE;
  public static final Map<TimeUnit, TimeBucket> TIME_UNIT_TO_TIME_BUCKET;

  static
  {
    Map<String, TimeBucket> bucketToType = Maps.newHashMap();
    Map<TimeUnit, TimeBucket> timeUnitToTimeBucket = Maps.newHashMap();

    for(TimeBucket timeBucket: TimeBucket.values())
    {
      timeUnitToTimeBucket.put(timeBucket.getTimeUnit(), timeBucket);
      bucketToType.put(timeBucket.getText(), timeBucket);
    }

    BUCKET_TO_TYPE = Collections.unmodifiableMap(bucketToType);
    TIME_UNIT_TO_TIME_BUCKET = Collections.unmodifiableMap(timeUnitToTimeBucket);
  }

  private String text;
  private TimeUnit timeUnit;

  TimeBucket(String text, TimeUnit timeUnit)
  {
    setText(text);
    setTimeUnit(timeUnit);
  }

  private void setText(String text)
  {
    Preconditions.checkNotNull(text);
    this.text = text;
  }

  private void setTimeUnit(TimeUnit timeUnit)
  {
    this.timeUnit = timeUnit;
  }

  public String getText()
  {
    return text;
  }

  public TimeUnit getTimeUnit()
  {
    return timeUnit;
  }

  public long roundDown(long timestamp)
  {
    if(timeUnit == null) {
      return 0;
    }

    long millis = timeUnit.toMillis(1L);
    return (timestamp / millis) * millis;
  }

  public static TimeBucket getBucket(String name)
  {
    return BUCKET_TO_TYPE.get(name);
  }

  public static TimeBucket getBucketEx(String name)
  {
    TimeBucket bucket = getBucket(name);
    Preconditions.checkState(bucket != null,
                             name + " is not a valid bucket type.");
    return bucket;
  }
}
