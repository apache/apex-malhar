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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.demos.dimensions.schemas.AdsTimeRangeBucket;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;


/**
 * <p>InputItemGenerator class.</p>
 *
 * @displayName Input Item Generator
 * @category Input
 * @tags generator, input operator
 * @since 0.3.2
 */
public class InputItemGenerator implements InputOperator
{
  @Min(1)
  private int numPublishers = 4;
  @Min(1)
  private int numAdvertisers = 3;
  @Min(1)
  private int numAdUnits = 3;
  private double expectedClickThruRate = 0.005;
  @Min(1)
  private int blastCount = 30000;
  private final Random random = new Random();
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

  public int getNumPublishers()
  {
    return numPublishers;
  }

  public void setNumPublishers(int numPublishers)
  {
    this.numPublishers = numPublishers;
  }

  public int getNumAdvertisers()
  {
    return numAdvertisers;
  }

  public void setNumAdvertisers(int numAdvertisers)
  {
    this.numAdvertisers = numAdvertisers;
  }

  public int getNumAdUnits()
  {
    return numAdUnits;
  }

  public void setNumAdUnits(int numAdUnits)
  {
    this.numAdUnits = numAdUnits;
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

  private int nextRandomId(int max)
  {
    return random.nextInt(max);
    /*int id;
    do {
      id = (int)Math.abs(Math.round(random.nextGaussian() * max / 2));
    }
    while (id >= max);
    return id;*/
  }

  @Override
  public void emitTuples()
  {
    try {
      long timestamp;
      for (int i = 0; i < blastCount; ++i) {
        int advertiserId = nextRandomId(numAdvertisers);
        //int publisherId = (advertiserId * 10 / numAdvertisers) * numPublishers / 10 + nextRandomId(numPublishers / 10);
        int publisherId = nextRandomId(numPublishers);
        int adUnit = random.nextInt(numAdUnits);

        advertiserId++;
        publisherId++;
        adUnit++;
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


  public void emitTuple(AdInfo adInfo) {
    this.outputPort.emit(adInfo);
  }

  private void buildAndSend(boolean click, int publisherId, int advertiserId, int adUnit, double value, long timestamp)
  {
    AdInfo adInfo = new AdInfo();
    adInfo.setPublisherId(publisherId);
    adInfo.setAdvertiserId(advertiserId);
    adInfo.setAdUnit(adUnit);
    if (click) {
      adInfo.revenue = value;
      adInfo.clicks = 1;
    }
    else {
      adInfo.cost = value;
      adInfo.impressions = 1;
    }
    adInfo.setTimestamp(timestamp);
    emitTuple(adInfo);
  }

}
