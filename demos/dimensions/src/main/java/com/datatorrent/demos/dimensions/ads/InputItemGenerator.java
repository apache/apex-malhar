/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.demos.dimensions.InputGenerator;
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
 * @category Test Bench
 * @since 3.1.0
 */

public class InputItemGenerator implements InputGenerator<AdInfo>
{
  private String eventSchemaJSON;
  private transient DimensionalConfigurationSchema schema;

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

  public List<Object> publisherName;
  public List<Object> advertiserName;
  public List<Object> locationName;

  private double hourOffset;
  private double dayOffset;
  private double expectedClickThruRate;
  private long currentMinute;
  private long currentDay;
  private long currentHour;
  @Min(1)
  private int blastCount = 30000;
  @Min(1)
  private int numTuplesPerWindow = 1000;
  private transient int windowCount = 0;
  private final Random random = new Random(0);
  public final transient DefaultOutputPort<AdInfo> outputPort = new DefaultOutputPort<AdInfo>();

  private double[] publisherScaleArray;
  private double[] publisherOffsetArray;
  private double[] advertiserScaleArray;
  private double[] advertiserOffsetArray;
  private double[] locationScaleArray;
  private double[] locationOffsetArray;

  @Override
  public void setup(OperatorContext context)
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();

    schema = new DimensionalConfigurationSchema(eventSchemaJSON,
                                        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    publisherID = schema.getKeysToEnumValuesList().get(PUBLISHER).size();
    if(advertiserName == null) {
      advertiserID = schema.getKeysToEnumValuesList().get(ADVERTISER).size();
    }
    else {
      advertiserID = advertiserName.size();
    }
    locationID = schema.getKeysToEnumValuesList().get(LOCATION).size();

    publisherName = schema.getKeysToEnumValuesList().get(PUBLISHER);
    if(advertiserName == null) {
      advertiserName = schema.getKeysToEnumValuesList().get(ADVERTISER);
    }
    locationName = schema.getKeysToEnumValuesList().get(LOCATION);

    publisherScaleArray = new double[publisherID];
    initializeScaleArray(publisherScaleArray);
    advertiserScaleArray = new double[advertiserID];
    initializeScaleArray(advertiserScaleArray);
    locationScaleArray = new double[locationID];
    initializeScaleArray(locationScaleArray);

    publisherOffsetArray = new double[publisherID];
    advertiserOffsetArray = new double[advertiserID];
    locationOffsetArray = new double[locationID];
  }

  @Override
  public void beginWindow(long windowId)
  {
    windowCount = 0;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    long time = System.currentTimeMillis();
    long nextMinute = time % 60000;
    long nextDay = time % (24 * 60 * 60 * 1000);
    long nextHour = time % (60 * 60 * 1000);
    double dayTimeOffset = .2 * (Math.cos(2.0 * Math.PI / (24.0 * 60.0 * 60.0 * 1000.0) * ((double) nextDay)) *.5 + 1.0);

    if(nextDay != currentDay) {
      currentDay = nextDay;
      dayOffset = random.nextDouble() * .05;
    }

    if(nextHour != currentHour) {
      currentHour = nextHour;
      hourOffset = random.nextDouble() * .05;
    }

    if(nextMinute != currentMinute) {
      expectedClickThruRate = random.nextDouble() * .1 + .1;
      currentMinute = nextMinute;

      double totalMinuteOffset = random.nextDouble() * .4;

      double publisherCut = random.nextDouble();
      double advertiserCut = random.nextDouble();
      double locationCut = random.nextDouble();

      double sectionCutTotal = publisherCut + advertiserCut + locationCut;
      publisherCut *= totalMinuteOffset / sectionCutTotal;
      advertiserCut *= totalMinuteOffset / sectionCutTotal;
      locationCut *= totalMinuteOffset / sectionCutTotal;

      initializeOffsetArray(publisherOffsetArray, publisherScaleArray, publisherCut);
      initializeOffsetArray(advertiserOffsetArray, advertiserScaleArray, advertiserCut);
      initializeOffsetArray(locationOffsetArray, locationScaleArray, locationCut);
    }

    long timestamp;
    for(int i = 0; i < blastCount && windowCount < numTuplesPerWindow; ++i, windowCount++) {
      int advertiserId = nextRandomId(advertiserID);
      int publisherId = nextRandomId(publisherID);
      int adUnit = nextRandomId(locationID);

      timestamp = System.currentTimeMillis();

      double tempOffset = publisherOffsetArray[publisherId]
                          + advertiserOffsetArray[advertiserId]
                          + locationScaleArray[adUnit];
      double tempScale = publisherScaleArray[publisherId]
                         * advertiserScaleArray[advertiserId]
                         * locationScaleArray[adUnit];

      double cost = 0.05 * random.nextDouble() * tempScale
                    + dayTimeOffset + dayOffset + hourOffset + tempOffset;

      buildAndSend(false, publisherId, advertiserId, adUnit, cost, timestamp);

      if(random.nextDouble() < expectedClickThruRate) {
        double revenue = 0.5 * random.nextDouble() * tempScale
                         + dayTimeOffset + dayOffset + hourOffset + 5.0 * tempOffset;
        // generate fake click
        buildAndSend(true, publisherId, advertiserId, adUnit, revenue, timestamp);
      }
    }
  }

  private void buildAndSend(boolean click, int publisherId, int advertiserId, int adUnit, double value, long timestamp)
  {
    AdInfo adInfo = new AdInfo();

    adInfo.setPublisher((String) publisherName.get(publisherId));
    adInfo.publisherID = publisherId;
    adInfo.setAdvertiser((String) advertiserName.get(advertiserId));
    adInfo.advertiserID = advertiserId;
    adInfo.setLocation((String) locationName.get(adUnit));
    adInfo.locationID = adUnit;

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

  public void emitTuple(AdInfo adInfo) {
    this.outputPort.emit(adInfo);
  }

  private void initializeScaleArray(double[] scaleArray)
  {
    for(int index = 0;
        index < scaleArray.length;
        index++) {
      scaleArray[index] = 1.0 + random.nextDouble() * 3;
    }
  }

  private void initializeOffsetArray(double[] offsetArray, double[] scaleArray, double total)
  {
    double tempTotal = 0;

    for(int index = 0;
        index < offsetArray.length;
        index++) {
      offsetArray[index] = random.nextDouble() * scaleArray[index];
      tempTotal += offsetArray[index];
    }

    for(int index = 0;
        index < offsetArray.length;
        index++) {
      offsetArray[index] *= total / tempTotal;
    }
  }

  private int nextRandomId(int max)
  {
    int id;
    do {
      id = (int)Math.abs(Math.round(random.nextGaussian() * max / 2));
    }
    while (id >= max);
    return id;
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

  public int getBlastCount()
  {
    return blastCount;
  }

  public void setBlastCount(int blastCount)
  {
    this.blastCount = blastCount;
  }

  @Override
  public OutputPort<AdInfo> getOutputPort()
  {
    return outputPort;
  }

  private static final Logger LOG = LoggerFactory.getLogger(InputItemGenerator.class);
}
