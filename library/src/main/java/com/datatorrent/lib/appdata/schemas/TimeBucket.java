/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public enum TimeBucket
{
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
    Preconditions.checkNotNull(timeUnit);
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
