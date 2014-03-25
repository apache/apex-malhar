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

import java.util.Random;

import javax.validation.constraints.Min;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;


/**
 * <p>InputItemGenerator class.</p>
 *
 * @since 0.3.2
 */
public class InputItemGenerator implements InputOperator
{
  @Min(1)
  private int numPublishers = 50;
  @Min(1)
  private int numAdvertisers = 100;
  @Min(1)
  private int numAdUnits = 5;
  private double expectedClickThruRate = 0.005;
  @Min(1)
  private int blastCount = 10000;
  private final Random random = new Random();
  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<AdInfo> outputPort = new DefaultOutputPort<AdInfo>();

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
    int id;
    do {
      id = (int)Math.abs(Math.round(random.nextGaussian() * max / 2));
    }
    while (id >= max);
    return id;
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

        double cost = 0.5 + 0.25 * random.nextDouble();
        timestamp = System.currentTimeMillis();

        /* 0 (zero) is used as the invalid value */
        emitTuple(false, publisherId + 1, advertiserId + 1, adUnit + 1, cost, timestamp);

        if (random.nextDouble() < expectedClickThruRate) {
          double revenue = 0.5 + 0.5 * random.nextDouble();
          timestamp = System.currentTimeMillis();
          // generate fake click
          emitTuple(true, publisherId + 1, advertiserId + 1, adUnit + 1, revenue, timestamp);
        }
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void emitTuple(boolean click, int publisherId, int advertiserId, int adUnit, double value, long timestamp)
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
    this.outputPort.emit(adInfo);
  }

}
