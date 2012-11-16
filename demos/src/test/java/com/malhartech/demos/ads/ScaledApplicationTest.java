/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.ads;

import com.malhartech.stram.StramLocalCluster;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


/**
 * Test the DAG declaration in local mode.
 */
public class ScaledApplicationTest {

  @Test
  public void testJavaConfig() throws IOException, Exception {
    ScaledApplication app = new ScaledApplication();
    app.setUnitTestMode(); // terminate quickly
    //app.setLocalMode(); // terminate with a long run
    StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.setHeartbeatMonitoringEnabled(false);
    //lc.setPerContainerBufferServer(true);
    lc.run();
  }
}
