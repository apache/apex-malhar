/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.ads;

import com.datatorrent.demos.ads.Application;
import com.malhartech.api.LocalMode;
import com.malhartech.stram.DAGPropertiesBuilder;
import com.malhartech.stram.StramLocalCluster;
import java.io.IOException;
import javax.validation.ConstraintViolationException;
import junit.framework.Assert;
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
    String tplgFile = "src/main/resources/ctr.app.properties";
    StramLocalCluster lc = new StramLocalCluster(DAGPropertiesBuilder.create(new Configuration(false), tplgFile));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }

  @Test
  public void testJavaConfig() throws IOException, Exception {
    LocalMode lma = LocalMode.newInstance();

    Application app = new Application();
    app.setUnitTestMode(); // terminate quickly
    //app.setLocalMode(); // terminate with a long run
    app.populateDAG(lma.getDAG(), new Configuration(false));
    try {
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(20000);
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
