/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.demos.yahoofinance.ApplicationWithSQL;
import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class ApplicationWithSQLTest
{
  public ApplicationWithSQLTest()
  {
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    ApplicationWithSQL topology = new ApplicationWithSQL();
    final StramLocalCluster lc = new StramLocalCluster(topology.getApplication(new Configuration(false)));

//    new Thread("LocalClusterController")
//    {
//      @Override
//      public void run()
//      {
//        try {
//          while(true) {
//          Thread.sleep(1000);
//
//          }
//        }
//        catch (InterruptedException ex) {
//        }
//
//        lc.shutdown();
//      }
//    }.start();
    long start = System.currentTimeMillis();
    lc.run();
    long end = System.currentTimeMillis();
    long time = end -start;
    System.out.println("Test used "+time+" ms");
  }
}
