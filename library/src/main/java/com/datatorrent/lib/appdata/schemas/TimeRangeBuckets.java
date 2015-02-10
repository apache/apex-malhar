/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class TimeRangeBuckets
{
  private String from;
  private String to;
  private List<String> intervals;

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
   * @return the intervals
   */
  public List<String> getIntervals()
  {
    return intervals;
  }

  /**
   * @param intervals the intervals to set
   */
  public void setIntervals(List<String> intervals)
  {
    this.intervals = intervals;
  }
}
