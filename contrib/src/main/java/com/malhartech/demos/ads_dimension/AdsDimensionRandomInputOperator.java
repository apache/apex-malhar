/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.ads_dimension;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.io.SimpleSinglePortInputOperator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class AdsDimensionRandomInputOperator extends SimpleSinglePortInputOperator<Map<String, Object>> implements Runnable
{
  private transient AtomicInteger lineCount = new AtomicInteger();
  private static final Logger LOG = LoggerFactory.getLogger(AdsDimensionRandomInputOperator.class);
  private int numAdvertisers = 200;
  private int numPublishers = 50;
  private double expectedClickThruRate = 0.02;
  private Random random = new Random();

  @Override
  public void endWindow()
  {
    System.out.println("Number of log lines: " + lineCount);
    lineCount.set(0);
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
  public void run()
  {
    try {
      int lineno = 0;
      while (true) {
        int advertiserId = nextRandomId(numAdvertisers);
        int publisherId = (advertiserId * 10 / numAdvertisers) * numPublishers / 10 + nextRandomId(numPublishers / 10);
        int adUnit = random.nextInt(5);

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("lineno", ++lineno);
        map.put("timestamp", System.currentTimeMillis());
        map.put("adv_id", advertiserId);
        map.put("pub_id", publisherId);
        map.put("adunit", adUnit);
        map.put("view", 1);
        map.put("cost", 0.5 + 0.25 * random.nextDouble());
        map.put("revenue", 0.5 + 0.1 * random.nextDouble());
        this.outputPort.emit(map);
        lineCount.incrementAndGet();

        map = new HashMap<String, Object>();
        if (random.nextDouble() < expectedClickThruRate) {
          Thread.sleep(random.nextInt(50));
          // generate fake click
          map.put("lineno", ++lineno);
          map.put("timestamp", System.currentTimeMillis());
          map.put("adv_id", advertiserId);
          map.put("pub_id", publisherId);
          map.put("adunit", adUnit);
          map.put("click", 1);
          map.put("cost", 0);
          map.put("revenue", 0.5 + 0.5 * random.nextDouble());
          this.outputPort.emit(map);
          lineCount.incrementAndGet();
        }
        Thread.sleep(random.nextInt(100));
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
