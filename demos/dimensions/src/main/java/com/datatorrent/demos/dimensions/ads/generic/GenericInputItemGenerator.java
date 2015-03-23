/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.demos.dimensions.ads.InputItemGenerator;
import com.datatorrent.demos.dimensions.schemas.AdsKeys;
import com.datatorrent.demos.dimensions.schemas.AdsSchemaResult;
import com.datatorrent.demos.dimensions.schemas.AdsTimeRangeBucket;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericInputItemGenerator implements InputOperator
{
  private double expectedClickThruRate = 0.005;
  @Min(1)
  private int blastCount = 30000;
  private final Random random = new Random();
  public final transient DefaultOutputPort<GenericAdInfo> outputPort = new DefaultOutputPort<GenericAdInfo>();

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
  }

  @Override
  public void endWindow()
  {
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
      for (int i = 0; i < blastCount; ++i) {
        int advertiserId = random.nextInt(AdsSchemaResult.ADVERTISERS.length) + 1;
        //int publisherId = (advertiserId * 10 / numAdvertisers) * numPublishers / 10 + nextRandomId(numPublishers / 10);
        int publisherId = random.nextInt(AdsSchemaResult.PUBLISHERS.length) + 1;
        int adUnit = random.nextInt(AdsSchemaResult.LOCATIONS.length) + 1;

        timestamp = System.currentTimeMillis();

        if(i == blastCount - 1) {
          logger.info("advertiserId {}, publisherId {}, addUnit {}, timestamp {}",
                      advertiserId, publisherId, adUnit, AdsTimeRangeBucket.sdf.format(new Date(timestamp)));
        }

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


  public void emitTuple(GenericAdInfo adInfo) {
    this.outputPort.emit(adInfo);
  }

  private void buildAndSend(boolean click, int publisherId, int advertiserId, int adUnit, double value, long timestamp)
  {
    GenericAdInfo adInfo = new GenericAdInfo();

    adInfo.setPublisher(AdsKeys.ID_TO_PUBLISHER.get(publisherId));
    adInfo.setAdvertiser(AdsKeys.ID_TO_ADVERTISER.get(advertiserId));
    adInfo.setLocation(AdsKeys.ID_TO_LOCATION.get(adUnit));
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

}
