/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.ResultSerializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleResultSerializer;
import java.util.Date;
import java.util.List;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QRType(type=AdsOneTimeResult.TYPE)
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class AdsOneTimeResult extends Result
{
  public static final String TYPE = "oneTimeData";
  private List<AdsOneTimeData> data;

  public AdsOneTimeResult()
  {
  }

  public AdsOneTimeResult(Query query)
  {
    super(query);
  }

  public AdsOneTimeResult(AdsOneTimeQuery query)
  {
    super(query);
  }

  /**
   * @return the data
   */
  public List<AdsOneTimeData> getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(List<AdsOneTimeData> data)
  {
    this.data = data;
  }

  public static class AdsOneTimeData
  {
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private String time;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private String advertiser;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private String publisher;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private String location;

    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private Long impressions;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private Long clicks;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private Double cost;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private Double revenue;

    public AdsOneTimeData()
    {
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

    public void setAdvertiserId(Integer id)
    {
      this.advertiser = AdsKeys.ID_TO_ADVERTISER.get(id);
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

    public void setPublisherId(Integer id)
    {
      this.publisher = AdsKeys.ID_TO_PUBLISHER.get(id);
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

    public void setLocationId(Integer id)
    {
      this.location = AdsKeys.ID_TO_LOCATION.get(id);
    }

    /**
     * @return the impressions
     */
    public Long getImpressions()
    {
      return impressions;
    }

    /**
     * @param impressions the impressions to set
     */
    public void setImpressions(Long impressions)
    {
      this.impressions = impressions;
    }

    /**
     * @return the clicks
     */
    public Long getClicks()
    {
      return clicks;
    }

    /**
     * @param clicks the clicks to set
     */
    public void setClicks(Long clicks)
    {
      this.clicks = clicks;
    }

    /**
     * @return the cost
     */
    public Double getCost()
    {
      return cost;
    }

    /**
     * @param cost the cost to set
     */
    public void setCost(Double cost)
    {
      this.cost = cost;
    }

    /**
     * @return the revenue
     */
    public Double getRevenue()
    {
      return revenue;
    }

    /**
     * @param revenue the revenue to set
     */
    public void setRevenue(Double revenue)
    {
      this.revenue = revenue;
    }

    /**
     * @return the time
     */
    public String getTime()
    {
      return time;
    }

    /**
     * @param time the time to set
     */
    public void setTime(String time)
    {
      this.time = time;
    }

    public void setTimeLong(long time)
    {
      this.time = AdsTimeRangeBucket.sdf.format(new Date(time));
    }
  }
}