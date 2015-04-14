/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.dedup;

public class SimpleEvent
{
  int id;
  double metric;
  String hhmm;

  public String getHhmm()
  {
    return hhmm;
  }

  public void setHhmm(String hhmm)
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
