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

import java.io.Serializable;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

/**
 * This represents a {@link TimeBucket} which can be a multiple of a time unit.
 *
 * @since 3.2.0
 */
public class CustomTimeBucket implements Serializable, Comparable<CustomTimeBucket>
{
  private static final long serialVersionUID = 201509221545L;

  public static final String TIME_BUCKET_NAME_REGEX = "(\\d+)([a-zA-Z]+)";
  public static final Pattern TIME_BUCKET_NAME_PATTERN = Pattern.compile(TIME_BUCKET_NAME_REGEX);

  private TimeBucket timeBucket;
  private long count;
  private String text;
  private long numMillis;

  private CustomTimeBucket()
  {
    //For kryo
  }

  public CustomTimeBucket(String timeBucketText)
  {
    if (timeBucketText.equals(TimeBucket.ALL.getText())) {
      initialize(TimeBucket.ALL, 0L);
    } else {
      Matcher matcher = TIME_BUCKET_NAME_PATTERN.matcher(timeBucketText);

      if (!matcher.matches()) {
        throw new IllegalArgumentException("The given text for the variable time bucket " + timeBucketText
                                           + " does not match the regex for a variable time bucket " + TIME_BUCKET_NAME_REGEX);
      }

      String amountString = matcher.group(1);
      long amount = Long.parseLong(amountString);

      String suffix = matcher.group(2);
      @SuppressWarnings("LocalVariableHidesMemberVariable")
      TimeBucket timeBucket = TimeBucket.getTimeBucketForSuffixEx(suffix);

      initialize(timeBucket, amount);
    }
  }

  public CustomTimeBucket(TimeBucket timeBucket, long count)
  {
    initialize(timeBucket, count);
  }

  public CustomTimeBucket(TimeBucket timeBucket)
  {
    if (timeBucket == TimeBucket.ALL) {
      initialize(timeBucket, 0L);
    } else {
      initialize(timeBucket, 1L);
    }
  }

  private void initialize(TimeBucket timeBucket, long count)
  {
    this.timeBucket = Preconditions.checkNotNull(timeBucket);
    this.count = count;

    if (timeBucket != TimeBucket.ALL) {
      Preconditions.checkArgument(count > 0, "The TimeBucket cannot be ALL.");
    } else {
      Preconditions.checkArgument(count == 0, "The count must be zero for the all TimeBucket.");
    }

    if (timeBucket != TimeBucket.ALL) {
      text = count + timeBucket.getSuffix();
      numMillis = timeBucket.getTimeUnit().toMillis(1) * count;
    } else {
      text = TimeBucket.ALL.getText();
    }
  }

  public boolean isUnit()
  {
    return count == 1;
  }

  public TimeBucket getTimeBucket()
  {
    return timeBucket;
  }

  public long getCount()
  {
    return count;
  }

  public long getNumMillis()
  {
    return numMillis;
  }

  public long toMillis(long multCount)
  {
    return numMillis * multCount;
  }

  /**
   * Rounds down the given time stamp to the nearest {@link TimeUnit} corresponding
   * to this TimeBucket.
   *
   * @param timestamp The timestamp to round down.
   * @return The rounded down timestamp.
   */
  public long roundDown(long timestamp)
  {
    if (timeBucket == TimeBucket.ALL) {
      return 0;
    }

    return (timestamp / numMillis) * numMillis;
  }

  public String getText()
  {
    return text;
  }

  @Override
  public String toString()
  {
    if (timeBucket == TimeBucket.ALL) {
      return TimeBucket.ALL.getText();
    } else {
      return count + timeBucket.getSuffix();
    }
  }

  @Override
  public int hashCode()
  {
    int hash = 3;
    hash = 97 * hash + Objects.hashCode(this.timeBucket);
    hash = 97 * hash + (int)(this.count ^ (this.count >>> 32));
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final CustomTimeBucket other = (CustomTimeBucket)obj;
    if (this.timeBucket != other.timeBucket) {
      return false;
    }
    if (this.count != other.count) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(CustomTimeBucket other)
  {
    if (this.numMillis < other.numMillis) {
      return -1;
    } else if (this.numMillis == other.numMillis) {
      return 0;
    }

    return 1;
  }
}
