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
package org.apache.apex.malhar.lib.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * <p>TimeBucketKey class.</p>
 *
 * @since 0.3.2
 */
public class TimeBucketKey
{

  private static final int TIMESPEC_MINUTE = 1;
  private static final int TIMESPEC_HOUR = 2;
  private static final int TIMESPEC_DAY = 4;
  private static final int TIMESPEC_WEEK = 8;
  private static final int TIMESPEC_MONTH = 16;
  private static final int TIMESPEC_YEAR = 32;

  public static final int TIMESPEC_YEAR_SPEC = TIMESPEC_YEAR;
  public static final int TIMESPEC_MONTH_SPEC = TIMESPEC_YEAR_SPEC | TIMESPEC_MONTH;
  public static final int TIMESPEC_WEEK_SPEC = TIMESPEC_YEAR_SPEC | TIMESPEC_WEEK;
  public static final int TIMESPEC_DAY_SPEC = TIMESPEC_MONTH_SPEC | TIMESPEC_DAY;
  public static final int TIMESPEC_HOUR_SPEC = TIMESPEC_DAY_SPEC | TIMESPEC_HOUR;
  public static final int TIMESPEC_MINUTE_SPEC = TIMESPEC_HOUR_SPEC | TIMESPEC_MINUTE;

  private static DateFormat yearDateFormat = new SimpleDateFormat("'Y|'yyyy");
  private static DateFormat monthDateFormat = new SimpleDateFormat("'M|'yyyyMM");
  private static DateFormat weekDateFormat = new SimpleDateFormat("'W|'yyyyww");
  private static DateFormat dayDateFormat = new SimpleDateFormat("'D|'yyyyMMdd");
  private static DateFormat hourDateFormat = new SimpleDateFormat("'h|'yyyyMMddHH");
  private static DateFormat minuteDateFormat = new SimpleDateFormat("'m|'yyyyMMddHHmm");

  private static final long MILLIS_IN_MIN = 60 * 1000;
  private static final long MILLIS_IN_HOUR = 60 * 60 * 1000;
  private static final long MILLIS_IN_DAY = 24 * 60 * 60 * 1000;
  private static final long MILLIS_IN_WEEK = 7 * 24 * 60 * 60 * 1000;

  static {
    // TODO - Fix this
    TimeZone tz = TimeZone.getTimeZone("GMT");
    yearDateFormat.setTimeZone(tz);
    monthDateFormat.setTimeZone(tz);
    weekDateFormat.setTimeZone(tz);
    dayDateFormat.setTimeZone(tz);
    hourDateFormat.setTimeZone(tz);
    minuteDateFormat.setTimeZone(tz);
  }

  private Calendar time;
  private int timeSpec;

  public TimeBucketKey()
  {
  }

  public TimeBucketKey(Calendar time, int timeSpec)
  {
    this.time = time;
    this.timeSpec = timeSpec;
  }

  public Calendar getTime()
  {
    return time;
  }

  public void setTime(Calendar time)
  {
    this.time = time;
  }

  public int getTimeSpec()
  {
    return timeSpec;
  }

  public void setTimeSpec(int timeSpec)
  {
    this.timeSpec = timeSpec;
  }

  @Override
  public int hashCode()
  {
    int hashcode = 0;
    if ((timeSpec & TIMESPEC_YEAR) != 0) {
      // Reducing year space by discounting previous years
      int year = time.get(Calendar.YEAR);
      hashcode += ((year - 2000) << 22);
    }
    if ((timeSpec & TIMESPEC_MONTH) != 0) {
      // Sharing same space with week
      hashcode += (time.get(Calendar.MONTH) << 16);
    }
    if ((timeSpec & TIMESPEC_WEEK) != 0) {
      hashcode += (time.get(Calendar.WEEK_OF_YEAR)  << 16);
    }
    if ((timeSpec & TIMESPEC_DAY) != 0) {
      hashcode += (time.get(Calendar.DAY_OF_MONTH) << 11);
    }
    if ((timeSpec & TIMESPEC_HOUR) != 0) {
      hashcode += (time.get(Calendar.HOUR_OF_DAY) << 6);
    }
    if ((timeSpec & TIMESPEC_MINUTE) != 0) {
      hashcode += time.get(Calendar.MINUTE);
    }
    return hashcode;
  }

  @Override
  public boolean equals(Object obj)
  {
    boolean equal = false;
    if (obj instanceof TimeBucketKey) {
      TimeBucketKey ckey = (TimeBucketKey)obj;
      if (timeSpec == TIMESPEC_MINUTE_SPEC) {
        equal = ((time.getTimeInMillis() / MILLIS_IN_MIN) == (ckey.getTime().getTimeInMillis() / MILLIS_IN_MIN));
      } else if (timeSpec == TIMESPEC_HOUR_SPEC) {
        equal = ((time.getTimeInMillis() / MILLIS_IN_HOUR) == (ckey.getTime().getTimeInMillis() / MILLIS_IN_HOUR));
      } else if (timeSpec == TIMESPEC_DAY_SPEC) {
        equal = ((time.getTimeInMillis() / MILLIS_IN_DAY) == (ckey.getTime().getTimeInMillis() / MILLIS_IN_DAY));
      } else if (timeSpec == TIMESPEC_WEEK_SPEC) {
        equal = ((time.getTimeInMillis() / MILLIS_IN_WEEK) == (ckey.getTime().getTimeInMillis() / MILLIS_IN_WEEK));
      } else {
        boolean chkEqual = true;
        if ((timeSpec & TIMESPEC_YEAR) != 0) {
          if (time.get(Calendar.YEAR) != ckey.getTime().get(Calendar.YEAR)) {
            chkEqual = false;
          }
        }
        if (chkEqual && ((timeSpec & TIMESPEC_MONTH) != 0)) {
          if (time.get(Calendar.MONTH) != ckey.getTime().get(Calendar.MONTH)) {
            chkEqual = false;
          }
        }
        equal = chkEqual;
      }
    }
    return equal;
  }

  @Override
  public String toString()
  {
    Date date = time.getTime();
    if (timeSpec == TIMESPEC_YEAR_SPEC) {
      return yearDateFormat.format(date);
    } else if (timeSpec == TIMESPEC_MONTH_SPEC) {
      return monthDateFormat.format(date);
    } else if (timeSpec == TIMESPEC_WEEK_SPEC) {
      return weekDateFormat.format(date);
    } else if (timeSpec == TIMESPEC_DAY_SPEC) {
      return dayDateFormat.format(date);
    } else if (timeSpec == TIMESPEC_HOUR_SPEC) {
      return hourDateFormat.format(date);
    } else if (timeSpec == TIMESPEC_MINUTE_SPEC) {
      return minuteDateFormat.format(date);
    }
    return null;
  }

}
