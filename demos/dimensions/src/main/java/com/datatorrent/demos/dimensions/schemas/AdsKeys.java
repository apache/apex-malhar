/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsKeys
{
  public static final Map<String, Integer> ADVERTISER_TO_ID;
  public static final Map<String, Integer> PUBLISHER_TO_ID;
  public static final Map<String, Integer> LOCATION_TO_ID;

  public static final Map<Integer, String> ID_TO_ADVERTISER;
  public static final Map<Integer, String> ID_TO_PUBLISHER;
  public static final Map<Integer, String> ID_TO_LOCATION;

  static {
    Map<String, Integer> advertiserToId = Maps.newHashMap();
    Map<Integer, String> idToAdvertiser = Maps.newHashMap();
    populateMap(advertiserToId,
                idToAdvertiser,
                AdsSchemaResult.ADVERTISERS,
                AdsSchemaResult.ADVERTISER_ALL);
    ADVERTISER_TO_ID = Collections.unmodifiableMap(advertiserToId);
    ID_TO_ADVERTISER = Collections.unmodifiableMap(idToAdvertiser);
    Map<String, Integer> publisherToId = Maps.newHashMap();
    Map<Integer, String> idToPublisher = Maps.newHashMap();
    populateMap(publisherToId,
                idToPublisher,
                AdsSchemaResult.PUBLISHERS,
                AdsSchemaResult.PUBLISHER_ALL);
    PUBLISHER_TO_ID = Collections.unmodifiableMap(publisherToId);
    ID_TO_PUBLISHER = Collections.unmodifiableMap(idToPublisher);
    Map<String, Integer> locationToId = Maps.newHashMap();
    Map<Integer, String> idToLocation = Maps.newHashMap();
    populateMap(locationToId,
                idToLocation,
                AdsSchemaResult.LOCATIONS,
                AdsSchemaResult.LOCATION_ALL);
    LOCATION_TO_ID = Collections.unmodifiableMap(locationToId);
    ID_TO_LOCATION = Collections.unmodifiableMap(idToLocation);
  }

  private static void populateMap(Map<String, Integer> propertyToId,
                                  Map<Integer, String> idToProperty,
                                  String[] propertyValues,
                                  String allVals)
  {
    propertyToId.put(null, 0);
    idToProperty.put(0, allVals);
    for(int idCounter = 0;
        idCounter < propertyValues.length;
        idCounter++) {
      propertyToId.put(propertyValues[idCounter], idCounter + 1);
      idToProperty.put(idCounter + 1, propertyValues[idCounter]);
    }
  }

  private String advertiser;
  private String publisher;
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

  @JsonIgnore
  public int getAdvertiserId()
  {
    return ADVERTISER_TO_ID.get(advertiser);
  }

  /**
   * @param advertiser the advertiser to set
   */
  public void setAdvertiser(String advertiser)
  {
    this.advertiser = advertiser;
  }

  public void setAdvertiserId(Integer advertiser)
  {
    this.advertiser = ID_TO_ADVERTISER.get(advertiser);
  }

  /**
   * @return the publisher
   */
  public String getPublisher()
  {
    return publisher;
  }

  @JsonIgnore
  public int getPublisherId()
  {
    return PUBLISHER_TO_ID.get(publisher);
  }

  /**
   * @param publisher the publisher to set
   */
  public void setPublisher(String publisher)
  {
    this.publisher = publisher;
  }

  public void setPublisherId(Integer publisher)
  {
    this.publisher = ID_TO_PUBLISHER.get(publisher);
  }

  /**
   * @return the location
   */
  public String getLocation()
  {
    return location;
  }

  @JsonIgnore
  public int getLocationId()
  {
    return LOCATION_TO_ID.get(location);
  }

  /**
   * @param location the location to set
   */
  public void setLocation(String location)
  {
    this.location = location;
  }

  public void setLocationId(Integer location)
  {
    this.location = ID_TO_LOCATION.get(location);
  }
}
