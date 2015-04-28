/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.google.common.collect.Maps;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Random;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class InputItemGenerator implements InputOperator
{
  public static final String[] PUBLISHERS = {"twitter", "facebook", "yahoo",
                                             "google", "bing", "amazon"};
  public static final String[] ADVERTISERS = {"starbucks", "safeway", "mcdonalds",
                                              "macys", "taco bell", "walmart", "khol's",
                                              "san diego zoo", "pandas", "jack in the box",
                                              "tomatina", "ron swanson"};
  public static final String[] LOCATIONS = {"N", "LREC", "SKY",
                                            "AL", "AK", "AZ",
                                            "AR", "CA", "CO",
                                            "CT", "DE", "FL",
                                            "GA", "HI", "ID"};

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
                ADVERTISERS);
    ADVERTISER_TO_ID = Collections.unmodifiableMap(advertiserToId);
    ID_TO_ADVERTISER = Collections.unmodifiableMap(idToAdvertiser);
    Map<String, Integer> publisherToId = Maps.newHashMap();
    Map<Integer, String> idToPublisher = Maps.newHashMap();
    populateMap(publisherToId,
                idToPublisher,
                PUBLISHERS);
    PUBLISHER_TO_ID = Collections.unmodifiableMap(publisherToId);
    ID_TO_PUBLISHER = Collections.unmodifiableMap(idToPublisher);
    Map<String, Integer> locationToId = Maps.newHashMap();
    Map<Integer, String> idToLocation = Maps.newHashMap();
    populateMap(locationToId,
                idToLocation,
                LOCATIONS);
    LOCATION_TO_ID = Collections.unmodifiableMap(locationToId);
    ID_TO_LOCATION = Collections.unmodifiableMap(idToLocation);
  }

  private static void populateMap(Map<String, Integer> propertyToId,
                                  Map<Integer, String> idToProperty,
                                  String[] propertyValues)
  {
    for(int idCounter = 0;
        idCounter < propertyValues.length;
        idCounter++) {
      propertyToId.put(propertyValues[idCounter], idCounter);
      idToProperty.put(idCounter, propertyValues[idCounter]);
    }
  }

  private double expectedClickThruRate = .2;//0.015;
  @Min(1)
  private int blastCount = 30000;
  @Min(1)
  private int numTuplesPerWindow = 1000;
  private transient int windowCount = 0;
  private final Random random = new Random(0);
  public final transient DefaultOutputPort<AdInfo> outputPort = new DefaultOutputPort<AdInfo>();

  private static final Logger logger = LoggerFactory.getLogger(InputItemGenerator.class);

  public double getExpectedClickThruRate()
  {
    return expectedClickThruRate;
  }

  public void setExpectedClickThruRate(double expectedClickThruRate)
  {
    this.expectedClickThruRate = expectedClickThruRate;
  }

  public int getBlastCount()
  {
    return blastCount;
  }

  public void setBlastCount(int blastCount)
  {
    this.blastCount = blastCount;
  }

  @Override
  public void beginWindow(long windowId)
  {
    windowCount = 0;
  }

  @Override
  public void endWindow()
  {
    logger.info("Current time stamp {}", System.currentTimeMillis());
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    try {
      long timestamp;
      for (int i = 0; i < blastCount && windowCount < numTuplesPerWindow; ++i, windowCount++) {
        int advertiserId = random.nextInt(ADVERTISERS.length);
        //int advertiserId = random.nextInt(1) + 1;
        //int publisherId = (advertiserId * 10 / numAdvertisers) * numPublishers / 10 + nextRandomId(numPublishers / 10);
        int publisherId = random.nextInt(PUBLISHERS.length);
        //int publisherId = random.nextInt(1) + 1;
        int adUnit = random.nextInt(LOCATIONS.length);
        //int adUnit = random.nextInt(1) + 1;

        timestamp = System.currentTimeMillis();

        double cost = 0.5 + 0.25 * random.nextDouble();

        /* 0 (zero) is used as the invalid value */
        buildAndSend(false, publisherId, advertiserId, adUnit, cost, timestamp);

        if (random.nextDouble() < expectedClickThruRate) {
          double revenue = 0.5 + 0.5 * random.nextDouble();
          // generate fake click
          buildAndSend(true, publisherId, advertiserId, adUnit, revenue, timestamp);
        }
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


  public void emitTuple(AdInfo adInfo) {
    this.outputPort.emit(adInfo);
  }

  private void buildAndSend(boolean click, int publisherId, int advertiserId, int adUnit, double value, long timestamp)
  {
    AdInfo adInfo = new AdInfo();

    adInfo.setPublisher(ID_TO_PUBLISHER.get(publisherId));
    adInfo.setAdvertiser(ID_TO_ADVERTISER.get(advertiserId));
    adInfo.setLocation(ID_TO_LOCATION.get(adUnit));
    if (click) {
      adInfo.setRevenue(value);
      adInfo.setClicks(1L);
    }
    else {
      adInfo.setCost(value);
      adInfo.setImpressions(1);
    }
    adInfo.setTime(timestamp);
    emitTuple(adInfo);
  }

  /**
   * @return the numTuplesPerWindow
   */
  public int getNumTuplesPerWindow()
  {
    return numTuplesPerWindow;
  }

  /**
   * @param numTuplesPerWindow the numTuplesPerWindow to set
   */
  public void setNumTuplesPerWindow(int numTuplesPerWindow)
  {
    this.numTuplesPerWindow = numTuplesPerWindow;
  }

}
