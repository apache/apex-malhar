/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.ads;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.malhartech.stram.StramLocalCluster;


/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest5 {

  @Test
  public void testJavaConfig() throws IOException, Exception {
    Application5 app = new Application5();
    app.setUnitTestMode(); // terminate quickly
    //app.setLocalMode(); // terminate with a long run
    StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }

}
