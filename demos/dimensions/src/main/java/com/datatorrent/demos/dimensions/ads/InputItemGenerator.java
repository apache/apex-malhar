/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class InputItemGenerator implements InputOperator
{
  private String eventSchemaJSON;
  private DimensionalConfigurationSchema schema;

  public static final String PUBLISHER = "publisher";
  public static final String ADVERTISER = "advertiser";
  public static final String LOCATION = "location";

  public static final String IMPRESSIONS = "impressions";
  public static final String CLICKS = "clicks";
  public static final String COST = "cost";
  public static final String REVENUE = "revenue";

  private int publisherID;
  private int advertiserID;
  private int locationID;

  private List<Object> publisherName;
  private List<Object> advertiserName;
  private List<Object> locationName;

  private double expectedClickThruRate = 0.015;
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
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();

    schema = new DimensionalConfigurationSchema(eventSchemaJSON,
                                        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    publisherID = schema.getKeysToValuesList().get(PUBLISHER).size();
    advertiserID = schema.getKeysToValuesList().get(ADVERTISER).size();
    locationID = schema.getKeysToValuesList().get(LOCATION).size();

    publisherName = schema.getKeysToValuesList().get(PUBLISHER);
    advertiserName = schema.getKeysToValuesList().get(ADVERTISER);
    locationName = schema.getKeysToValuesList().get(LOCATION);
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
        int advertiserId = random.nextInt(advertiserID);
        //int advertiserId = random.nextInt(1) + 1;
        //int publisherId = (advertiserId * 10 / numAdvertisers) * numPublishers / 10 + nextRandomId(numPublishers / 10);
        int publisherId = random.nextInt(publisherID);
        //int publisherId = random.nextInt(1) + 1;
        int adUnit = random.nextInt(locationID);
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

    adInfo.setPublisher((String) publisherName.get(publisherId));
    adInfo.setAdvertiser((String) advertiserName.get(advertiserId));
    adInfo.setLocation((String) locationName.get(adUnit));

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

  /**
   * @return the eventSchemaJSON
   */
  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  /**
   * @param eventSchemaJSON the eventSchemaJSON to set
   */
  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
  }
}
