/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.ads;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.ads.Application;
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
