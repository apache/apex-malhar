/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.schemas.TimeRangeBucket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsTimeRangeBucket extends TimeRangeBucket
{
  public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

  public long getFromLong()
  {
    try {
      return sdf.parse(this.getFrom()).getTime();
    }
    catch(ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  public long getToLong()
  {
    try {
      return sdf.parse(this.getTo()).getTime();
    }
    catch(ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  public TimeUnit getTimeUnit()
  {
    if(this.getBucket().equals("1m")) {
      return TimeUnit.MINUTES;
    }
    else if(this.getBucket().equals("1h")) {
      return TimeUnit.HOURS;
    }
    else if(this.getBucket().equals("1d")) {
      return TimeUnit.DAYS;
    }
    else {
      throw new UnsupportedOperationException("Cannot get a Time unit for " + this.getBucket());
    }
  }
}
