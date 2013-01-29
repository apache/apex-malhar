///*
// *  Copyright (c) 2012-2013 Malhar, Inc.
// *  All Rights Reserved.
// */
//package com.malhartech.demos.yahoofinance;
//
//import com.malhartech.stram.StramLocalCluster;
//import org.apache.hadoop.conf.Configuration;
//import org.junit.Test;
//
///**
// * Run Yahoo Finance application demo.
// *
// * @author Locknath Shil <locknath@malhar-inc.com>
// */
//public class YahooFinanceTest
//{
//
//  /**
//   * This will run for ever.
//   *
//   * @throws Exception
//   */
//  @Test
//  public void testApplication() throws Exception
//  {
//    YahooFinanceApplication app = new YahooFinanceApplication();
//    StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
//    lc.setHeartbeatMonitoringEnabled(false);
//    lc.run();
//  }
//
//  /**
//   * This will run for specified sleep time.
//   *
//   * @throws Exception
//   */
// // @Test
//  public void testApplication2() throws Exception
//  {
//    YahooFinanceApplication app = new YahooFinanceApplication();
//    final StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
//    new Thread()
//    {
//      @Override
//      public void run()
//      {
//        try {
//          Thread.sleep(20000);
//        }
//        catch (InterruptedException ex) {
//        }
//        lc.shutdown();
//      }
//    }.start();
//
//    lc.run();
//  }
//}
