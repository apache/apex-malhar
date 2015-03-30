/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads.schemas;

import javax.validation.constraints.NotNull;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class TimeRangeBucket
{
  @NotNull
  private String from;
  @NotNull
  private String to;
  @NotNull
  private String bucket;

  public TimeRangeBucket()
  {
  }

  /**
   * @return the from
   */
  public String getFrom()
  {
    return from;
  }

  /**
   * @param from the from to set
   */
  public void setFrom(String from)
  {
    this.from = from;
  }

  /**
   * @return the to
   */
  public String getTo()
  {
    return to;
  }

  /**
   * @param to the to to set
   */
  public void setTo(String to)
  {
    this.to = to;
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
