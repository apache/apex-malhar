/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.mobile;

import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


public class ApplicationObsoleteTest
{
  public ApplicationObsoleteTest()
  {
  }

  /**
   * Test of getApplication method, of class ApplicationObsolete.
   */
  @Test
  public void testGetApplication() throws Exception
  {
    ApplicationObsolete app = new ApplicationObsolete();
    StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }
}
