/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.template;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.malhartech.stram.StramLocalCluster;
import com.malhartech.template.Application;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  @Test
  public void testApplication() throws IOException, Exception {
    Application app = new Application();
    try {
      StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
