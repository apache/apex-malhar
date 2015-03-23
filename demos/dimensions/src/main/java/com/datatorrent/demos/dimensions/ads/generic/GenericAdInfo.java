/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads.generic;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericAdInfo
{
  private String publisher;
  private String advertiser;
  private String location;
  private double cost = 0.0;
  private double revenue = 0.0;
  private long impressions = 0;
  private long clicks = 0;
  private long time = 0;

  public GenericAdInfo()
  {
  }

  public GenericAdInfo(String publisher,
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
}
