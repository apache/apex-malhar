/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.DTHashingStrategy;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.UnifiableAggregate;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputation.Aggregator;
import java.io.Serializable;

import java.util.concurrent.TimeUnit;

public class AdInfo implements Serializable
{
  private static final long serialVersionUID = 201505250652L;

  public String publisher;
  public int publisherID;
  public String advertiser;
  public int advertiserID;
  public String location;
  public int locationID;
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
        aggregate.publisherID = aggregatorInput.publisherID;
      }

      if (advertiserId) {
        aggregate.advertiser = aggregatorInput.advertiser;
        aggregate.advertiserID = aggregatorInput.advertiserID;
      }

      if (adUnit) {
        aggregate.location = aggregatorInput.location;
        aggregate.locationID = aggregatorInput.locationID;
      }

      aggregate.dimensionsDescriptorID = this.dimensionsDescriptorID;
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
    int dimensionsDescriptorID;

    public void init(String dimension, int dimensionsDescriptorID)
    {
      String[] attributes = dimension.split(":");
      for (String attribute : attributes) {
        String[] keyval = attribute.split("=", 2);
        String key = keyval[0];
        if (key.equals("time")) {
          time = TimeUnit.valueOf(keyval[1]);
          timeBucket = TimeBucket.TIME_UNIT_TO_TIME_BUCKET.get(time);
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

      this.dimensionsDescriptorID = dimensionsDescriptorID;
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
        event.publisherID = src.publisherID;
      }
      else {
        event.publisherID = -1;
      }

      if (advertiserId) {
        event.advertiser = src.advertiser;
        event.advertiserID = src.advertiserID;
      }
      else {
        event.advertiserID = -1;
      }

      if (adUnit) {
        event.location = src.location;
        event.locationID = src.locationID;
      }
      else {
        event.locationID = -1;
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
        hash = 71 * hash + event.publisherID;
      }

      if (advertiserId) {
        hash = 71 * hash + event.advertiserID;
      }

      if (adUnit) {
        hash = 71 * hash + event.locationID;
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

      if (publisherId && event1.publisherID != event2.publisherID) {
        return false;
      }

      if (advertiserId && event1.advertiserID != event2.advertiserID) {
        return false;
      }

      if (adUnit && event1.locationID != event2.locationID) {
        return false;
      }

      return true;
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201402211829L;
  }

  public static class AdInfoHashingStrategy implements DTHashingStrategy<AdInfoAggregateEvent>
  {
    public AdInfoHashingStrategy()
    {
    }

    @Override
    public int computeHashCode(AdInfoAggregateEvent t)
    {
      int hash = 5;
      hash = 71 * hash + t.publisherID;
      hash = 71 * hash + t.advertiserID;
      hash = 71 * hash + t.locationID;
      hash = 71 * hash + (int)t.time;
      hash = 71 * hash + t.timeBucket;

      return hash;
    }

    @Override
    public boolean equals(AdInfoAggregateEvent t, AdInfoAggregateEvent t1)
    {
      return t.publisherID == t1.publisherID &&
             t.advertiserID == t1.advertiserID &&
             t.locationID == t1.locationID &&
             t.time == t1.time &&
             t.timeBucket == t1.timeBucket;
    }
  }

  public static class AdInfoAggregateEvent extends AdInfo implements DimensionsComputation.AggregateEvent, UnifiableAggregate
  {
    private static final long serialVersionUID = 1L;
    int aggregatorIndex;
    public int timeBucket;
    private int dimensionsDescriptorID;

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

    /**
     * @return the dimensionsDescriptorID
     */
    public int getDimensionsDescriptorID()
    {
      return dimensionsDescriptorID;
    }

    /**
     * @param dimensionsDescriptorID the dimensionsDescriptorID to set
     */
    public void setDimensionsDescriptorID(int dimensionsDescriptorID)
    {
      this.dimensionsDescriptorID = dimensionsDescriptorID;
    }


    @Override
    public int hashCode()
    {
      int hash = 5;
      hash = 71 * hash + this.publisherID;
      hash = 71 * hash + this.advertiserID;
      hash = 71 * hash + this.locationID;
      hash = 71 * hash + (int)this.time;
      hash = 71 * hash + this.timeBucket;

      return hash;
    }

    @Override
    public boolean equals(Object o)
    {
      if(o == null || !(o instanceof AdInfoAggregateEvent)) {
        return false;
      }

      AdInfoAggregateEvent aae = (AdInfoAggregateEvent) o;

      return this.publisherID == aae.publisherID &&
             this.advertiserID == aae.advertiserID &&
             this.locationID == aae.locationID &&
             this.time == aae.time &&
             this.timeBucket == aae.timeBucket;
    }


  }
}
