/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.summit.ads;

import com.datatorrent.contrib.summit.ads.AdInfo;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 *
 */
public class InputGenerator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(InputGenerator.class);
  private int numPublishers = 50;
  private int numAdvertisers = 100;
  private int numAdUnits = 5;
  private double expectedClickThruRate = 0.005;
  private int blastCount = 10000;
  private Random random = new Random();

  private static final int dimSelect[][] = { {0, 0, 0}, {0, 0, 1}, {0, 1, 0}, {1, 0, 0}, {0, 1, 1}, {1, 0, 1}, {1, 1, 0}, {1, 1, 1} };
  private static final int dimSelLen = 8;

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
      for (int i = 0; i <blastCount; ++i) {
        int advertiserId = nextRandomId(numAdvertisers);
        //int publisherId = (advertiserId * 10 / numAdvertisers) * numPublishers / 10 + nextRandomId(numPublishers / 10);
        int publisherId = nextRandomId(numPublishers);
        int adUnit = random.nextInt(numAdUnits);

        double cost = 0.5 + 0.25 * random.nextDouble();
        timestamp = System.currentTimeMillis();

        emitDimensions(false, publisherId, advertiserId, adUnit, cost, timestamp);

        if (random.nextDouble() < expectedClickThruRate) {
          double revenue = 0.5 + 0.5 * random.nextDouble();
          timestamp = System.currentTimeMillis();
          // generate fake click
          emitDimensions(true, publisherId, advertiserId, adUnit, revenue, timestamp);
        }
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void emitDimensions(boolean click, int publisherId, int advertiserId, int adUnit, double value, long timestamp) {
    int adkey;
    for (int j = 0; j < dimSelLen; ++j) {
      AdInfo adInfo = new AdInfo();
      if (dimSelect[j][0] == 1) adInfo.setPublisherId(publisherId);
      if (dimSelect[j][1] == 1) adInfo.setAdvertiserId(advertiserId);
      if (dimSelect[j][2] == 1) adInfo.setAdUnit(adUnit);
      adInfo.setClick(click);
      adInfo.setValue(value);
      adInfo.setTimestamp(timestamp);
      this.outputPort.emit(adInfo);
    }
  }
}
