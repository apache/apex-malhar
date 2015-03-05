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
package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.demos.dimensions.schemas.AdsSchemaResult;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputation.Aggregator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>AdInfo class.</p>
 *
 * @since 0.3.2
 */
public class AdInfo implements Serializable, Cloneable
{
  private static final Logger logger = LoggerFactory.getLogger(AdInfo.class);

  public static final int MINUTE_BUCKET = 0;
  public static final int HOUR_BUCKET = 1;
  public static final int DAY_BUCKET = 2;

  public static final Map<String, Integer> BUCKET_NAME_TO_INDEX;

  static
  {
    Map<String, Integer> bucketNameToIndex = Maps.newHashMap();
    bucketNameToIndex.put(AdsSchemaResult.BUCKETS[0], MINUTE_BUCKET);
    bucketNameToIndex.put(AdsSchemaResult.BUCKETS[1], HOUR_BUCKET);
    bucketNameToIndex.put(AdsSchemaResult.BUCKETS[2], DAY_BUCKET);

    BUCKET_NAME_TO_INDEX = Collections.unmodifiableMap(bucketNameToIndex);
  }

  public static final Map<Integer, TimeUnit> BUCKET_TO_TIMEUNIT;

  static
  {
    Map<Integer, TimeUnit> bucketToTimeunit = Maps.newHashMap();
    bucketToTimeunit.put(MINUTE_BUCKET, TimeUnit.MINUTES);
    bucketToTimeunit.put(HOUR_BUCKET, TimeUnit.HOURS);
    bucketToTimeunit.put(DAY_BUCKET, TimeUnit.DAYS);

    BUCKET_TO_TIMEUNIT = Collections.unmodifiableMap(bucketToTimeunit);
  }

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
  int bucket;

  public AdInfo()
  {
  }

  public AdInfo(AdInfo ai,
                int bucket)
  {
    this.publisherId = ai.publisherId;
    this.advertiserId = ai.advertiserId;
    this.adUnit = ai.adUnit;
    this.cost = ai.cost;
    this.revenue = ai.revenue;
    this.impressions = ai.impressions;
    this.clicks = ai.clicks;

    if(bucket == MINUTE_BUCKET) {
      this.timestamp = AdInfo.roundMinute(ai.timestamp);
    }
    else if(bucket == HOUR_BUCKET) {
      this.timestamp = AdInfo.roundHour(ai.timestamp);
    }
    else if(bucket == DAY_BUCKET) {
      this.timestamp = AdInfo.roundDay(ai.timestamp);
    }
    else {
      throw new IllegalArgumentException("Not a valid bucket.");
    }

    this.bucket = bucket;
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

  public double getCost()
  {
    return cost;
  }

  public double getRevenue()
  {
    return revenue;
  }

  public long getImpressions()
  {
    return impressions;
  }

  public long getClicks()
  {
    return clicks;
  }

  public void setClicks(long clicks) {
    this.clicks = clicks;
  }

  public void setCost(double cost) {
    this.cost = cost;
  }

  public void setRevenue(double revenue) {
    this.revenue = revenue;
  }

  public void setImpressions(long impressions) {
    this.impressions = impressions;
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 71 * hash + this.publisherId;
    hash = 71 * hash + this.advertiserId;
    hash = 71 * hash + this.adUnit;
    hash = 71 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
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
    final AdInfo other = (AdInfo) obj;
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

    return this.bucket == other.bucket;
  }

  @Override
  public String toString()
  {
    return "AdInfo{" + "publisherId=" + publisherId + ", advertiserId=" + advertiserId + ", adUnit=" + adUnit + ", timestamp=" + timestamp + ", cost=" + cost + ", revenue=" + revenue + ", impressions=" + impressions + ", clicks=" + clicks + '}';
  }

  public static long roundMinute(long timestamp)
  {
    long mm = TimeUnit.MINUTES.toMillis(1);
    return (timestamp / mm) * mm;
  }

  public static long roundMinuteUp(long timestamp)
  {
    long mm = TimeUnit.MINUTES.toMillis(1);

    long result = (timestamp / mm) * mm;
    long mod = timestamp % mm;

    if(mod < 0) {
      result -= mm;
    }
    else if(mod > 0) {
      result += mm;
    }

    return result;
  }

  public static long roundHour(long timestamp)
  {
    long hm = TimeUnit.HOURS.toMillis(1);
    return (timestamp / hm) * hm;
  }

  public static long roundHourUp(long timestamp)
  {
    long hm = TimeUnit.HOURS.toMillis(1);

    long result = (timestamp / hm) * hm;
    long mod = timestamp % hm;

    if(mod < 0) {
      result -= hm;
    }
    else if(mod > 0) {
      result += hm;
    }

    return result;
  }

  public static long roundDay(long timestamp)
  {
    long dm = TimeUnit.DAYS.toMillis(1);
    return (timestamp / dm) * dm;
  }

  public static long roundDayUp(long timestamp)
  {
    long dm = TimeUnit.DAYS.toMillis(1);

    long result = (timestamp / dm) * dm;
    long mod = timestamp % dm;

    if(mod < 0) {
      result -= dm;
    }
    else if(mod > 0) {
      result += dm;
    }

    return result;
  }

  /**
   * @return the bucket
   */
  public int getBucket()
  {
    return bucket;
  }

  /**
   * @param bucket the bucket to set
   */
  public void setBucket(int bucket)
  {
    this.bucket = bucket;
  }

  public static class AdInfoAggregator implements Aggregator<AdInfo, AdInfoAggregateEvent>
  {
    String dimension;
    TimeUnit time;
    boolean publisherId;
    boolean advertiserId;
    boolean adUnit;

    public void init(String dimension)
    {
      String[] attributes = dimension.split(":");
      boolean hasTime = false;
      for (String attribute : attributes) {
        String[] keyval = attribute.split("=", 2);
        String key = keyval[0];
        if (key.equals("time")) {
          time = TimeUnit.valueOf(keyval[1]);

          Preconditions.checkArgument(AdInfo.BUCKET_TO_TIMEUNIT.values().contains(time),
                                      time + " Must be a supported time unit: "  + AdInfo.BUCKET_TO_TIMEUNIT.values());
          hasTime = true;
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

      if(!hasTime) {
        throw new IllegalArgumentException("The time dimension must be specified.");
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

        if(time.equals(TimeUnit.MINUTES)) {
          event.timestamp = AdInfo.roundMinute(src.timestamp);
          event.bucket = AdInfo.MINUTE_BUCKET;
        }
        else if(time.equals(TimeUnit.HOURS)) {
          event.timestamp = AdInfo.roundHour(src.timestamp);
          event.bucket = AdInfo.HOUR_BUCKET;
        }
        else if(time.equals(TimeUnit.DAYS)) {
          event.timestamp = AdInfo.roundDay(src.timestamp);
          event.bucket = AdInfo.DAY_BUCKET;
        }
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

  public static class AdInfoAggregateEvent extends AdInfo implements DimensionsComputation.AggregateEvent
  {
    private static final long serialVersionUID = 1L;
    int aggregatorIndex;

    public AdInfoAggregateEvent()
    {
      //Used for kryo serialization
    }


    public AdInfoAggregateEvent(AdInfoAggregateEvent ai,
                                int bucket)
    {
      super(ai, bucket);
      this.aggregatorIndex = ai.aggregatorIndex;
    }

    public AdInfoAggregateEvent(AdInfoAggregateEvent ai,
                                int bucket,
                                int aggregatorIndex)
    {
      super(ai, bucket);
      this.aggregatorIndex = aggregatorIndex;
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
  }

  private static final long serialVersionUID = 201402211825L;
}
