/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class TimeBucket
{
  private String time;
  private String bucket;

  /**
   * @return the time
   */
  public String getTime()
  {
    return time;
  }

  /**
   * @param time the time to set
   */
  public void setTime(String time)
  {
    this.time = time;
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
