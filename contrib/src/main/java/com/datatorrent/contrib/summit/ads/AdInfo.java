/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.summit.ads;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class AdInfo
{
  
  Integer publisherId;
  Integer advertiserId;
  Integer adUnit;
  boolean click;
  double value;
  long timestamp;

  public AdInfo() {
  }

  public AdInfo(Integer publisherId, Integer advertiserId, Integer adUnit, boolean click, double value, long timestamp) {
    this.publisherId = publisherId;
    this.advertiserId = advertiserId;
    this.adUnit = adUnit;
    this.click = click;
    this.value = value;
    this.timestamp = timestamp;
  }

  public Integer getPublisherId()
  {
    return publisherId;
  }

  public void setPublisherId(Integer publisherId)
  {
    this.publisherId = publisherId;
  }

  public Integer getAdvertiserId()
  {
    return advertiserId;
  }

  public void setAdvertiserId(Integer advertiserId)
  {
    this.advertiserId = advertiserId;
  }

  public Integer getAdUnit()
  {
    return adUnit;
  }

  public void setAdUnit(Integer adUnit)
  {
    this.adUnit = adUnit;
  }

  public boolean isClick()
  {
    return click;
  }

  public void setClick(boolean click)
  {
    this.click = click;
  }

  public double getValue()
  {
    return value;
  }

  public void setValue(double value)
  {
    this.value = value;
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

}
