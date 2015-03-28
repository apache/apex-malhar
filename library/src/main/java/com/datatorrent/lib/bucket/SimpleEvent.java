/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.bucket;

public class SimpleEvent
{
  int id;
  double metric;
  long hhmm;

  public long getHhmm()
  {
    return hhmm;
  }

  public void setHhmm(long hhmm)
  {
    this.hhmm = hhmm;
  }

  public int getId()
  {
    return id;
  }

  public void setId(int id)
  {
    this.id = id;
  }

  public double getMetric()
  {
    return metric;
  }

  public void setMetric(double metric)
  {
    this.metric = metric;
  }




}
