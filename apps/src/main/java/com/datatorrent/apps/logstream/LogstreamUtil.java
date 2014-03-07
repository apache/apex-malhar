/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.logstream;

import com.datatorrent.common.util.DTThrowable;
import java.text.NumberFormat;
import java.text.ParseException;

/**
 * Utility class for common final values and methods used in logstream application
 *
 */
public class LogstreamUtil
{
  public static final int TIMEBUCKET_SECOND = 1;
  public static final int TIMEBUCKET_MINUTE = 2;
  public static final int TIMEBUCKET_HOUR = 4;
  public static final int TIMEBUCKET_DAY = 8;
  public static final int TIMEBUCKET_WEEK = 16;
  public static final int TIMEBUCKET_MONTH = 32;
  public static final int TIMEBUCKET_YEAR = 64;
  public static final String LOG_TYPE = "LOG_TYPE";
  public static final String FILTER = "FILTER";

  public enum AggregateOperation
  {
    SUM, AVERAGE, COUNT
  };

  public static long extractTime(long currentWindowId, long windowWidth)
  {
    long time;
    time = (currentWindowId >>> 32) * 1000 + windowWidth * (currentWindowId & 0xffffffffL);
    return time;
  }

  /**
   * returns the number value of the supplied object
   * @param value
   * @return
   */
  public static Number extractNumber(Object value)
  {
    NumberFormat numberFormat = NumberFormat.getInstance();
    if (value instanceof Number) {
      return (Number)value;
    }
    else if (value == null) {
      return new Long(0);
    }
    else {
      try {
        return numberFormat.parse(value.toString());
      }
      catch (ParseException ex) {
        DTThrowable.rethrow(ex);
      }
    }
    return new Long(0);
  }

  public static int extractTimeBucket(String bucket)
  {
    int timeBucket = 0;
    if (bucket.equals("s")) {
      timeBucket = TIMEBUCKET_SECOND;
    }
    else if (bucket.equals("m")) {
      timeBucket = TIMEBUCKET_MINUTE;
    }
    else if (bucket.equals("h")) {
      timeBucket = TIMEBUCKET_HOUR;
    }
    else if (bucket.equals("D")) {
      timeBucket = TIMEBUCKET_DAY;
    }
    else if (bucket.equals("W")) {
      timeBucket = TIMEBUCKET_WEEK;
    }
    else if (bucket.equals("M")) {
      timeBucket = TIMEBUCKET_MONTH;
    }
    else if (bucket.equals("Y")) {
      timeBucket = TIMEBUCKET_YEAR;
    }

    return timeBucket;

  }

}
