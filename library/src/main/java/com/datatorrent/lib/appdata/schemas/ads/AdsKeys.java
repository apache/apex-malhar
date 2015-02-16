/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.google.common.collect.Maps;
import java.util.Map;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsKeys
{
  public static final Map<String, Integer> ADVERTISER_TO_ID;
  public static final Map<String, Integer> PUBLISHER_TO_ID;
  public static final Map<String, Integer> LOCATION_TO_ID;

  static {
    Map<String, Integer> advertiserToId = Maps.newHashMap();
    Map<String, Integer> publisherToId = Maps.newHashMap();
    Map<String, Integer> locationToId = Maps.newHashMap();
  }
  
  private static void populateMap(Map<String, Integer> advertiserToId,
                                  String[] advertisers)
  {

  }

  private String advertiser;
  private String publisher;
  @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
  private String location;

  public AdsKeys()
  {
  }

  /**
   * @return the advertiser
   */
  public String getAdvertiser()
  {
    return advertiser;
  }

  public int getAdvertiserId()
  {

  }

  /**
   * @param advertiser the advertiser to set
   */
  public void setAdvertiser(String advertiser)
  {
    this.advertiser = advertiser;
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
}
