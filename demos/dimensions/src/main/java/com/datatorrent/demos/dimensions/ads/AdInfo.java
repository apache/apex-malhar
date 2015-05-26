/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.UnifiableAggregate;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputation.Aggregator;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdInfo
{
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

  public static class AdsDimensionsCombination extends AdInfoAggregator implements AbstractDimensionsComputation.DimensionsCombination<AdInfo, AdInfoAggregateEvent>
  {
    @Override
    public void setKeys(AdInfo aggregatorInput, AdInfoAggregateEvent aggregate)
    {
      if (time != null) {
        aggregate.timeBucket = timeBucketInt;
        aggregate.time = TimeUnit.MILLISECONDS.convert(time.convert(aggregatorInput.time, TimeUnit.MILLISECONDS), time);
      }

      if (publisherId) {
        aggregate.publisher = aggregatorInput.publisher;
      }

      if (advertiserId) {
        aggregate.advertiser = aggregatorInput.advertiser;
      }

      if (adUnit) {
        aggregate.location = aggregatorInput.location;
      }
    }
  }

  public static class AdInfoSumAggregator implements com.datatorrent.lib.dimensions.aggregator.Aggregator<AdInfo, AdInfoAggregateEvent>
  {
    @Override
    public void aggregate(AdInfoAggregateEvent dest, AdInfo src)
    {
      dest.clicks += src.clicks;
      dest.cost += src.cost;
      dest.impressions += src.impressions;
      dest.revenue += src.revenue;
    }

    @Override
    public void aggregate(AdInfoAggregateEvent dest, AdInfoAggregateEvent src)
    {
      dest.clicks += src.clicks;
      dest.cost += src.cost;
      dest.impressions += src.impressions;
      dest.revenue += src.revenue;
    }

    @Override
    public AdInfoAggregateEvent createDest(AdInfo first)
    {
      AdInfoAggregateEvent dest = new AdInfoAggregateEvent();
      dest.clicks = first.clicks;
      dest.cost = first.cost;
      dest.impressions = first.impressions;
      dest.revenue = first.revenue;

      return dest;
    }
  }

  public static class AdInfoAggregator implements Aggregator<AdInfo, AdInfoAggregateEvent>
  {
    String dimension;
    TimeBucket timeBucket;
    int timeBucketInt;
    TimeUnit time;
    boolean publisherId;
    boolean advertiserId;
    boolean adUnit;

    public void init(String dimension)
    {
      String[] attributes = dimension.split(":");
      for (String attribute : attributes) {
        String[] keyval = attribute.split("=", 2);
        String key = keyval[0];
        if (key.equals("time")) {
          timeBucket = TimeBucket.getBucket(keyval[1]);
          timeBucketInt = timeBucket.ordinal();
          time = timeBucket.getTimeUnit();
        }
        else if (key.equals("publisher")) {
          publisherId = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
        }
        else if (key.equals("advertiser")) {
          advertiserId = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
        }
        else if (key.equals("location")) {
          adUnit = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
        }
        else {
          throw new IllegalArgumentException("Unknown attribute '" + attribute + "' specified as part of dimension!");
        }
      }

      this.dimension = dimension;
    }

    /**
     * Dimension specification for display in operator properties.
     * @return The dimension.
     */
    public String getDimension()
    {
      return dimension;
    }

    @Override
    public String toString()
    {
      return dimension;
    }

    @Override
    public int hashCode()
    {
      int hash = 3;
      hash = 83 * hash + (this.time != null ? this.time.hashCode() : 0);
      hash = 83 * hash + (this.publisherId ? 1 : 0);
      hash = 83 * hash + (this.advertiserId ? 1 : 0);
      hash = 83 * hash + (this.adUnit ? 1 : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final AdInfoAggregator other = (AdInfoAggregator) obj;
      if (this.time != other.time) {
        return false;
      }
      if (this.publisherId != other.publisherId) {
        return false;
      }
      if (this.advertiserId != other.advertiserId) {
        return false;
      }
      return this.adUnit == other.adUnit;
    }

    @Override
    public AdInfoAggregateEvent getGroup(AdInfo src, int aggregatorIndex)
    {
      AdInfoAggregateEvent event = new AdInfoAggregateEvent(aggregatorIndex);
      if (time != null) {
        event.time = TimeUnit.MILLISECONDS.convert(time.convert(src.time, TimeUnit.MILLISECONDS), time);
      }

      if (publisherId) {
        event.publisher = src.publisher;
      }

      if (advertiserId) {
        event.advertiser = src.advertiser;
      }

      if (adUnit) {
        event.location = src.location;
      }

      return event;
    }

    @Override
    public void aggregate(AdInfoAggregateEvent dest, AdInfo src)
    {
      dest.cost += src.cost;
      dest.revenue += src.revenue;
      dest.impressions += src.impressions;
      dest.clicks += src.clicks;
    }

    @Override
    public void aggregate(AdInfoAggregateEvent dest, AdInfoAggregateEvent src)
    {
      dest.cost += src.cost;
      dest.revenue += src.revenue;
      dest.impressions += src.impressions;
      dest.clicks += src.clicks;
    }

    @Override
    public int computeHashCode(AdInfo event)
    {
      int hash = 5;

      if (publisherId) {
        hash = 71 * hash + event.publisher.hashCode();
      }

      if (advertiserId) {
        hash = 71 * hash + event.advertiser.hashCode();
      }

      if (adUnit) {
        hash = 71 * hash + event.location.hashCode();
      }

      if (time != null) {
        long ltime = time.convert(event.time, TimeUnit.MILLISECONDS);
        hash = 71 * hash + (int) (ltime ^ (ltime >>> 32));
      }

      return hash;
    }

    @Override
    public boolean equals(AdInfo event1, AdInfo event2)
    {
      if (event1 == event2) {
        return true;
      }

      if (event2 == null) {
        return false;
      }

      if (event1.getClass() != event2.getClass()) {
        return false;
      }

      if (time != null && time.convert(event1.time, TimeUnit.MILLISECONDS) != time.convert(event2.time, TimeUnit.MILLISECONDS)) {
        return false;
      }

      if (publisherId && !event1.publisher.equals(event2.publisher)) {
        return false;
      }

      if (advertiserId && !event1.advertiser.equals(event2.advertiser)) {
        return false;
      }

      if (adUnit && !event1.location.equals(event2.location)) {
        return false;
      }

      return true;
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201402211829L;
  }

  public static class AdInfoAggregateEvent extends AdInfo implements DimensionsComputation.AggregateEvent, UnifiableAggregate
  {
    private static final long serialVersionUID = 1L;
    int aggregatorIndex;
    int timeBucket;

    public AdInfoAggregateEvent()
    {
      //Used for kryo serialization
    }

    public AdInfoAggregateEvent(int aggregatorIndex)
    {
      this.aggregatorIndex = aggregatorIndex;
    }

    @Override
    public int getAggregatorIndex()
    {
      return aggregatorIndex;
    }

    @Override
    public int getAggregateIndex()
    {
      return aggregatorIndex;
    }

    @Override
    public void setAggregateIndex(int aggregateIndex)
    {
      this.aggregatorIndex = aggregateIndex;
    }
  }


}
