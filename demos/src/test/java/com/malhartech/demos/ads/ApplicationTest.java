/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.ads;

import com.malhartech.demos.ads.Application;
import com.malhartech.stram.DAGPropertiesBuilder;
import com.malhartech.stram.StramLocalCluster;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  @Ignore
  @Test
  public void testPropertiesConfig() throws IOException, Exception {
    String tplgFile = "src/main/resources/ctrapp.tplg.properties";
    StramLocalCluster lc = new StramLocalCluster(DAGPropertiesBuilder.create(new Configuration(false), tplgFile));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }

  @Test
  public void testJavaConfig() throws IOException, Exception {
    Application app = new Application();
    //app.setUnitTestMode(); // terminate quickly
    app.setLocalMode(); // terminate with a long run
    StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }

}
