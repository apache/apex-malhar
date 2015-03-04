/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsTimeRangeBucket extends TimeRangeBucket
{
  public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

  @JsonIgnore
  public long getFromLong()
  {
    try {
      return sdf.parse(this.getFrom()).getTime();
    }
    catch(ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  @JsonIgnore
  public void setFromLong(long from)
  {
    this.setFrom(sdf.format(new Date(from)));
  }

  @JsonIgnore
  public long getToLong()
  {
    try {
      return sdf.parse(this.getTo()).getTime();
    }
    catch(ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  @JsonIgnore
  public void setToLong(long to)
  {
    this.setTo(sdf.format(new Date(to)));
  }
}
