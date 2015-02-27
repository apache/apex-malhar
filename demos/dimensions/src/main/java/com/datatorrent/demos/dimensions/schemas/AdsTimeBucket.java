/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import java.text.ParseException;
import java.util.Date;
import org.codehaus.jackson.annotate.JsonIgnore;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsTimeBucket
{
  private String time;
  private String bucket;

  public AdsTimeBucket()
  {
  }

  /**
   * @return the time
   */
  public String getTime()
  {
    return time;
  }

  @JsonIgnore
  public long getTimeLong()
  {
    try {
      return AdsTimeRangeBucket.sdf.parse(getTime()).getTime();
    }
    catch(ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * @param time the time to set
   */
  public void setTime(String time)
  {
    this.time = time;
  }

  public void setTimeLong(long to)
  {
    setTime(AdsTimeRangeBucket.sdf.format(new Date(to)));
  }

  /**
   * @return the bucket
   */
  public String getBucket()
  {
    return bucket;
  }

  /**
   * @param bucket the bucket to set
   */
  public void setBucket(String bucket)
  {
    this.bucket = bucket;
  }
}
