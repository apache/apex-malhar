/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.adsdimension;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import com.datatorrent.lib.statistics.DimensionsComputation.Aggregator;

/**
 * <p>AdInfo class.</p>
 *
 * @since 0.3.2
 */
public class AdInfo implements Serializable
{
  /* dimension attributes */
  int publisherId;
  int advertiserId;
  int adUnit;
  long timestamp;
  /* metrics */
  double cost;
  double revenue;
  long impressions;
  long clicks;

  AdInfo()
  {
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

  public long getTimestamp()
  {
    return timestamp;
  }

  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 71 * hash + this.publisherId;
    hash = 71 * hash + this.advertiserId;
    hash = 71 * hash + this.adUnit;
    hash = 71 * hash + (int)(this.timestamp ^ (this.timestamp >>> 32));
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
    final AdInfo other = (AdInfo)obj;
    if (this.publisherId != other.publisherId) {
      return false;
    }
    if (this.advertiserId != other.advertiserId) {
      return false;
    }
    if (this.adUnit != other.adUnit) {
      return false;
    }
    if (this.timestamp != other.timestamp) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "AdInfo{" + "publisherId=" + publisherId + ", advertiserId=" + advertiserId + ", adUnit=" + adUnit + ", timestamp=" + timestamp + ", cost=" + cost + ", revenue=" + revenue + ", impressions=" + impressions + ", clicks=" + clicks + '}';
  }

  public static class AdInfoAggregator implements Aggregator<AdInfo>
  {
    String dimension;
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
          time = TimeUnit.valueOf(keyval[1]);
        }
        else if (key.equals("publisherId")) {
          publisherId = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
        }
        else if (key.equals("advertiserId")) {
          advertiserId = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
        }
        else if (key.equals("adUnit")) {
          adUnit = keyval.length == 1 || Boolean.parseBoolean(keyval[1]);
        }
        else {
          throw new IllegalArgumentException("Unknown attribute '" + attribute + "' specified as part of dimension!");
        }
      }

      this.dimension = dimension;
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
      final AdInfoAggregator other = (AdInfoAggregator)obj;
      if (this.time != other.time) {
        return false;
      }
      if (this.publisherId != other.publisherId) {
        return false;
      }
      if (this.advertiserId != other.advertiserId) {
        return false;
      }
      if (this.adUnit != other.adUnit) {
        return false;
      }
      return true;
    }

    @Override
    public AdInfo getGroup(AdInfo src)
    {
      AdInfo event = new AdInfo();
      if (time != null) {
        event.timestamp = TimeUnit.MILLISECONDS.convert(time.convert(src.timestamp, TimeUnit.MILLISECONDS), time);
      }

      if (publisherId) {
        event.publisherId = src.publisherId;
      }

      if (advertiserId) {
        event.advertiserId = src.advertiserId;
      }

      if (adUnit) {
        event.adUnit = src.adUnit;
      }

      return event;
    }

    @Override
    public void aggregate(AdInfo dest, AdInfo src)
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
        hash = 71 * hash + event.publisherId;
      }

      if (advertiserId) {
        hash = 71 * hash + event.advertiserId;
      }

      if (adUnit) {
        hash = 71 * hash + event.adUnit;
      }

      if (time != null) {
        long ltime = time.convert(event.timestamp, TimeUnit.MILLISECONDS);
        hash = 71 * hash + (int)(ltime ^ (ltime >>> 32));
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

      if (time != null && time.convert(event1.timestamp, TimeUnit.MILLISECONDS) != time.convert(event2.timestamp, TimeUnit.MILLISECONDS)) {
        return false;
      }

      if (publisherId && event1.publisherId != event2.publisherId) {
        return false;
      }

      if (advertiserId && event1.advertiserId != event2.advertiserId) {
        return false;
      }

      if (adUnit && event1.adUnit != event2.adUnit) {
        return false;
      }

      return true;
    }

    @SuppressWarnings("FieldNameHidesFieldInSuperclass")
    private static final long serialVersionUID = 201402211829L;
  }

  private static final long serialVersionUID = 201402211825L;
}
