/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.ads_dimension;

import com.malhartech.lib.io.SimpleSinglePortInputOperator;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
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

  @Override
  public void endWindow()
  {
    System.out.println("Number of log lines: " + lineCount);
    lineCount.set(0);
  }

  @Override
  public void run()
  {
    try {
      int lineno = 0;
      while (true) {
        ++lineno;
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("lineno", lineno);
        map.put("timestamp", System.currentTimeMillis());
        /*
        map.put("adv_id", advertiserId);
        map.put("pub_id", publisherId);
        map.put("adunit", adUnit);

        if () {
          map.put("click", 1);
        } else {
          map.put("view", 1);
        }

        map.put("cost", cost);
        map.put("revenue", revenue);
*/
        this.outputPort.emit(map);
        Thread.sleep(1);
        lineCount.incrementAndGet();
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
