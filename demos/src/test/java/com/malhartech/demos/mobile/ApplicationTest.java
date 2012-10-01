/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.mobile;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.malhartech.demos.mobile.Application;
import com.malhartech.stram.StramLocalCluster;


public class ApplicationTest
{
  public ApplicationTest()
  {
  }

  /**
   * Test of getApplication method, of class Application.
   */
  @Test
  public void testGetApplication() throws Exception
  {
    Application app = new Application();
    app.setUnitTestMode(); // terminate quickly
    StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }
}
