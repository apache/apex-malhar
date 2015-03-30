/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads.schemas;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class TimeRangeBuckets
{
  private String from;
  private String to;
  private List<String> buckets;

  public TimeRangeBuckets()
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
   * @return the buckets
   */
  public List<String> getBuckets()
  {
    return buckets;
  }

  /**
   * @param buckets the buckets to set
   */
  public void setBuckets(List<String> buckets)
  {
    this.buckets = buckets;
  }
}
