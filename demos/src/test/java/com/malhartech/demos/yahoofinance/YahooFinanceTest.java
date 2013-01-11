/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;


import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class YahooFinanceTest
{
  public YahooFinanceTest()
  {

  }

  //@Test
  public void testApplication() throws Exception
  {
    YahooFinanceApplication app = new YahooFinanceApplication();
    StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }

    @Test
  public void testApplication2() throws Exception
  {
    YahooFinanceApplication app = new YahooFinanceApplication();
    final StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
     new Thread() {
      @Override
      public void run()
      {
        try {
          Thread.sleep(10000);
        }
        catch (InterruptedException ex) {
        }
        lc.shutdown();
      }
    }.start();

    lc.run();
  }
}
