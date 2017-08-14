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
package org.apache.apex.malhar.lib.appdata.schemas;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This enum represents a TimeBucket that is supported by AppData
 * <br/>
 * <br/>
 * The currently supported buckets are:
 * <ul>
 * <li>s - second</li>
 * <li>m - minute</li>
 * <li>h - hour</li>
 * <li>d - day</li>
 * </ul>
 * <br/>
 * <br/>
 * Buckets supported in the future will be:
 * <ul>
 * <li>s - second</li>
 * <li>w - week</li>
 * <li>M - month</li>
 * <li>q - quarter</li>
 * <li>y - year</li>
 * </ul>
 * @since 3.0.0
 */
public enum TimeBucket
{
  //DO NOT change order of enums. Ordinal is used as id for storage.

  /**
   * No time bucketing.
   */
  ALL("all", null, null),
  /**
   * Second time bucketing.
   */
  SECOND("1s", TimeUnit.SECONDS, "s"),
  /**
   * Minute time bucketing.
   */
  MINUTE("1m", TimeUnit.MINUTES, "m"),
  /**
   * Hour time bucketing.
   */
  HOUR("1h", TimeUnit.HOURS, "h"),
  /**
   * Day time bucketing.
   */
  DAY("1d", TimeUnit.DAYS, "d");

  public static final String TIME_BUCKET_NAME_REGEX = "1[a-zA-Z]+";
  public static final Pattern TIME_BUCKET_NAME_PATTERN = Pattern.compile(TIME_BUCKET_NAME_REGEX);
  public static final Set<String> SUFFIXES;
  public static final Map<String, TimeBucket> SUFFIX_TO_TIME_BUCKET;

  /**
   * A map from the test/name of the bucket to the {@link TimeBucket}.
   */
  public static final Map<String, TimeBucket> BUCKET_TO_TYPE;
  /**
   * A map from a {@link TimeUnit} to the corresponding {@link TimeBucket}.
   */
  public static final Map<TimeUnit, TimeBucket> TIME_UNIT_TO_TIME_BUCKET;

  static {
    Map<String, TimeBucket> bucketToType = Maps.newHashMap();
    Map<TimeUnit, TimeBucket> timeUnitToTimeBucket = Maps.newHashMap();

    for (TimeBucket timeBucket : TimeBucket.values()) {
      timeUnitToTimeBucket.put(timeBucket.getTimeUnit(), timeBucket);
      bucketToType.put(timeBucket.getText(), timeBucket);
    }

    BUCKET_TO_TYPE = Collections.unmodifiableMap(bucketToType);
    TIME_UNIT_TO_TIME_BUCKET = Collections.unmodifiableMap(timeUnitToTimeBucket);

    Set<String> suffixes = Sets.newHashSet();
    Map<String, TimeBucket> suffixToTimeBucket = Maps.newHashMap();

    for (TimeBucket timeBucket: TimeBucket.values()) {
      suffixes.add(timeBucket.getSuffix());
      suffixToTimeBucket.put(timeBucket.getSuffix(), timeBucket);
    }

    SUFFIXES = Sets.newHashSet(suffixes);
    SUFFIX_TO_TIME_BUCKET = Maps.newHashMap(suffixToTimeBucket);
  }

  private final String suffix;
  private String text;
  private TimeUnit timeUnit;

  /**
   * Create a time bucket with the given corresponding text and {@link TimeUnit}
   * @param text The text or name corresponding to the TimeBucket.
   * @param timeUnit The {@link TimeUnit} that the TimeBucket represents.
   * @param suffix The suffix used to denote this {@link TimeBucket}
   */
  TimeBucket(String text, TimeUnit timeUnit, String suffix)
  {
    setText(text);
    setTimeUnit(timeUnit);
    this.suffix = suffix;
  }

  /**
   * Sets the name or text corresponding to a TimeBucket.
   * @param text The name or text corresponding to a TimeBucket.
   */
  private void setText(String text)
  {
    Preconditions.checkNotNull(text);
    this.text = text;
  }

  /**
   * The {@link TimeUnit} that a TimeBucket represents.
   * @param timeUnit The {@link TimeUnit} that a TimeBucket represents.
   */
  private void setTimeUnit(TimeUnit timeUnit)
  {
    this.timeUnit = timeUnit;
  }

  /**
   * This method gets the suffix for the {@link TimeBucket}.
   * @return The suffix for this {@link TimeBucket}.
   */
  public String getSuffix()
  {
    return this.suffix;
  }

  /**
   * Gets the name or text corresponding to this TimeBucket.
   * @return The name or text corresponding to this TimeBucket.
   */
  public String getText()
  {
    return text;
  }

  /**
   * Gets the {@link TimeUnit} corresponding to this TimeBucket.
   * @return The {@link TimeUnit} corresponding to this TimeBucket.
   */
  public TimeUnit getTimeUnit()
  {
    return timeUnit;
  }

  /**
   * Rounds down the given time stamp to the nearest {@link TimeUnit} corresponding
   * to this TimeBucket.
   * @param timestamp The timestamp to round down.
   * @return The rounded down timestamp.
   */
  public long roundDown(long timestamp)
  {
    if (timeUnit == null) {
      return 0;
    }

    long millis = timeUnit.toMillis(1L);
    return (timestamp / millis) * millis;
  }

  /**
   * Gets the TimeBucket with the corresponding text or name. Returns null if there is no
   * TimeBucket with the corresponding text or name.
   * @param name The text or name of the TimeBucket to retrieve.
   * @return The TimeBucket with the corresponding text or name, or null if there is no TimeBucket with
   * corresponding test or name.
   */
  public static TimeBucket getBucket(String name)
  {
    return BUCKET_TO_TYPE.get(name);
  }

  /**
   * Gets the TimeBucket with the corresponding text or name. An IllegalArgumentException is thrown if there is no
   * TimeBucket with the corresponding text or name.
   * @param name The text or name of the TimeBucket to retrieve.
   * @return The TimeBucket with the corresponding text or name, or an IllegalArgumentException is thrown if there is no TimeBucket with
   * corresponding test or name.
   */
  public static TimeBucket getBucketEx(String name)
  {
    TimeBucket bucket = getBucket(name);
    Preconditions.checkArgument(bucket != null, name + " is not a valid bucket type.");
    return bucket;
  }

  public static TimeBucket getTimeBucketForSuffixEx(String suffix)
  {
    Preconditions.checkNotNull(suffix);
    Preconditions.checkArgument(SUFFIXES.contains(suffix));
    return SUFFIX_TO_TIME_BUCKET.get(suffix);
  }

  public static class TimeBucketComparator implements Comparator<TimeBucket>
  {
    public static final TimeBucketComparator INSTANCE = new TimeBucketComparator();

    private TimeBucketComparator()
    {
    }

    @Override
    public int compare(TimeBucket timeBucketA, TimeBucket timeBucketB)
    {
      return timeBucketA.getTimeUnit().compareTo(timeBucketB.getTimeUnit());
    }
  }
}
