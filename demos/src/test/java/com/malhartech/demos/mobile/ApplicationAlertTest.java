/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.mobile;

import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


public class ApplicationAlertTest
{
  public ApplicationAlertTest()
  {
  }

  /**
   * Test of getApplication method, of class Application.
   */
  @Test
  public void testGetApplication() throws Exception
  {
    ApplicationAlert app = new ApplicationAlert();
    StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }
}
