/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.ads_dimension;

import com.datatorrent.contrib.ads_dimension.Application;
import com.malhartech.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ApplicationTest
{
  @Test
  public void testApplication() throws Exception
  {
    Application app = new Application();
    LocalMode lma = LocalMode.newInstance();
    app.populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }

}
