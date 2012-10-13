/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.performance;

import com.malhartech.stram.StramLocalCluster;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{
  @Test
  public void testApplication() throws IOException, Exception
  {
    Application app = new Application();
    final StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.setHeartbeatMonitoringEnabled(false);

//    new Thread("LocalClusterController")
//    {
//      @Override
//      public void run()
//      {
//        try {
//          Thread.sleep(10000);
//        }
//        catch (InterruptedException ex) {
//        }
//
//        lc.shutdown();
//      }
//    }.start();

    lc.run();
  }
}
