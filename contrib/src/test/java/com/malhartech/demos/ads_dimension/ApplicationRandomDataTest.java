/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.ads_dimension;

import com.malhartech.demos.ads_dimension.Application;
import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ApplicationRandomDataTest
{
  @Test
  public void testApplication() throws Exception
  {
    ApplicationRandomData app = new ApplicationRandomData();
    StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }

}
