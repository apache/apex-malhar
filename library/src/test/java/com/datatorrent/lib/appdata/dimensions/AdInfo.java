/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.dimensions;

import java.io.Serializable;

public class AdInfo implements Serializable
{
  private static final long serialVersionUID = 201505241039L;

  public String publisher;
  public String advertiser;
  public String location;
  public double cost = 0.0;
  public double revenue = 0.0;
  public long impressions = 0;
  public long clicks = 0;
  public long time = 0;

  public AdInfo()
  {
  }

  public AdInfo(String publisher,
                       String advertiser,
                       String location,
                       double cost,
                       double revenue,
                       long impressions,
                       long clicks,
                       long time)
  {
    this.publisher = publisher;
    this.advertiser = advertiser;
    this.location = location;
    this.cost = cost;
    this.revenue = revenue;
    this.impressions = impressions;
    this.clicks = clicks;
    this.time = time;
  }

  /**
   * @return the publisher
   */
  public String getPublisher()
  {
    return publisher;
  }

  /**
   * @param publisher the publisher to set
   */
  public void setPublisher(String publisher)
  {
    this.publisher = publisher;
  }

  /**
   * @return the advertiser
   */
  public String getAdvertiser()
  {
    return advertiser;
  }

  /**
   * @param advertiser the advertiser to set
   */
  public void setAdvertiser(String advertiser)
  {
    this.advertiser = advertiser;
  }

  /**
   * @return the location
   */
  public String getLocation()
  {
    return location;
  }

  /**
   * @param location the location to set
   */
  public void setLocation(String location)
  {
    this.location = location;
  }

  /**
   * @return the cost
   */
  public double getCost()
  {
    return cost;
  }

  /**
   * @param cost the cost to set
   */
  public void setCost(double cost)
  {
    this.cost = cost;
  }

  /**
   * @return the revenue
   */
  public double getRevenue()
  {
    return revenue;
  }

  /**
   * @param revenue the revenue to set
   */
  public void setRevenue(double revenue)
  {
    this.revenue = revenue;
  }

  /**
   * @return the impressions
   */
  public long getImpressions()
  {
    return impressions;
  }

  /**
   * @param impressions the impressions to set
   */
  public void setImpressions(long impressions)
  {
    this.impressions = impressions;
  }

  /**
   * @return the clicks
   */
  public long getClicks()
  {
    return clicks;
  }

  /**
   * @param clicks the clicks to set
   */
  public void setClicks(long clicks)
  {
    this.clicks = clicks;
  }

  /**
   * @return the time
   */
  public long getTime()
  {
    return time;
  }

  /**
   * @param time the time to set
   */
  public void setTime(long time)
  {
    this.time = time;
  }

  @Override
  public int hashCode()
  {
    int hash = 3;
    hash = 89 * hash + (this.publisher != null ? this.publisher.hashCode() : 0);
    hash = 89 * hash + (this.advertiser != null ? this.advertiser.hashCode() : 0);
    hash = 89 * hash + (this.location != null ? this.location.hashCode() : 0);
    hash = 89 * hash + (int)(Double.doubleToLongBits(this.cost) ^ (Double.doubleToLongBits(this.cost) >>> 32));
    hash = 89 * hash + (int)(Double.doubleToLongBits(this.revenue) ^ (Double.doubleToLongBits(this.revenue) >>> 32));
    hash = 89 * hash + (int)(this.impressions ^ (this.impressions >>> 32));
    hash = 89 * hash + (int)(this.clicks ^ (this.clicks >>> 32));
    hash = 89 * hash + (int)(this.time ^ (this.time >>> 32));
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if(obj == null) {
      return false;
    }
    if(getClass() != obj.getClass()) {
      return false;
    }
    final AdInfo other = (AdInfo)obj;
    if((this.publisher == null) ? (other.publisher != null) : !this.publisher.equals(other.publisher)) {
      return false;
    }
    if((this.advertiser == null) ? (other.advertiser != null) : !this.advertiser.equals(other.advertiser)) {
      return false;
    }
    if((this.location == null) ? (other.location != null) : !this.location.equals(other.location)) {
      return false;
    }
    if(Double.doubleToLongBits(this.cost) != Double.doubleToLongBits(other.cost)) {
      return false;
    }
    if(Double.doubleToLongBits(this.revenue) != Double.doubleToLongBits(other.revenue)) {
      return false;
    }
    if(this.impressions != other.impressions) {
      return false;
    }
    if(this.clicks != other.clicks) {
      return false;
    }
    if(this.time != other.time) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "AdInfo{" + "publisher=" + publisher + ", advertiser=" + advertiser + ", location=" + location + ", cost=" + cost + ", revenue=" + revenue + ", impressions=" + impressions + ", clicks=" + clicks + ", time=" + time + '}';
  }
}
