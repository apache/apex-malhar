/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.ads_dimension;

import com.malhartech.api.LocalMode;
import com.malhartech.demos.ads_dimension.Application;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ApplicationTest
{
  @Test
  public void testApplication() throws Exception
  {
    Application app = new Application();
    LocalMode lma = LocalMode.newInstance();
    app.getApplication(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }

}
