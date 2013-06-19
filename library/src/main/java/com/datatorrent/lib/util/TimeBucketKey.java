/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
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

  private static final DateFormat yearDateFormat = new SimpleDateFormat("'Y|'yyyy");
  private static final DateFormat monthDateFormat = new SimpleDateFormat("'M|'yyyyMM");
  private static final DateFormat weekDateFormat = new SimpleDateFormat("'W|'yyyyww");
  private static final DateFormat dayDateFormat = new SimpleDateFormat("'D|'yyyyMMdd");
  private static final DateFormat hourDateFormat = new SimpleDateFormat("'h|'yyyyMMddHH");
  private static final DateFormat minuteDateFormat = new SimpleDateFormat("'m|'yyyyMMddHHmm");

  private Calendar time;
  private int timeSpec;

  public TimeBucketKey() {
  }

  public TimeBucketKey(Calendar time, int timeSpec) {
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
      if (chkEqual && ((timeSpec & TIMESPEC_WEEK) != 0)) {
        if (time.get(Calendar.WEEK_OF_YEAR) != ckey.getTime().get(Calendar.WEEK_OF_YEAR)) {
          chkEqual = false;
        }
      }
      if (chkEqual && ((timeSpec & TIMESPEC_DAY) != 0)) {
        if (time.get(Calendar.DAY_OF_MONTH) != ckey.getTime().get(Calendar.DAY_OF_MONTH)) {
          chkEqual = false;
        }
      }
      if (chkEqual && ((timeSpec & TIMESPEC_HOUR) != 0)) {
        if (time.get(Calendar.HOUR_OF_DAY) != ckey.getTime().get(Calendar.HOUR_OF_DAY)) {
          chkEqual = false;
        }
      } else if (chkEqual && ((timeSpec & TIMESPEC_MINUTE) != 0)) {
        if (time.get(Calendar.MINUTE) != ckey.getTime().get(Calendar.MINUTE)) {
          chkEqual = false;
        }
      }
      equal = chkEqual;
    }
    return equal;
  }

  @Override
  public String toString()
  {
    Date date = time.getTime();
    if (timeSpec == TIMESPEC_YEAR_SPEC) {
      return yearDateFormat.format(date);
    }else if (timeSpec == TIMESPEC_MONTH_SPEC) {
      return monthDateFormat.format(date);
    }else if (timeSpec == TIMESPEC_WEEK_SPEC) {
      return weekDateFormat.format(date);
    }else if (timeSpec == TIMESPEC_DAY_SPEC) {
      return dayDateFormat.format(date);
    }else if (timeSpec == TIMESPEC_HOUR_SPEC) {
      return hourDateFormat.format(date);
    }else if (timeSpec == TIMESPEC_MINUTE_SPEC) {
      return minuteDateFormat.format(date);
    }
    return null;
  }




}
