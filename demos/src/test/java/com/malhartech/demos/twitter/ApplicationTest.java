/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.demos.twitter.SynchronousFeedApplication;
import com.malhartech.demos.twitter.Application;
import com.malhartech.stram.DAGPropertiesBuilder;
import com.malhartech.stram.StramLocalCluster;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{
  @Ignore
  @Test
  public void testPropertiesConfig() throws IOException, Exception
  {
    String tplgFile = "src/main/resources/ctrapp.tplg.properties";
    StramLocalCluster lc = new StramLocalCluster(DAGPropertiesBuilder.create(new Configuration(false), tplgFile));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }

  @Ignore
  @Test
  public void testAsyncApplication() throws IOException, Exception
  {
    Application app = new Application();
    if (app == null) {
      Assert.fail("could not get reference to the application");
    }
    StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }

  @Test
  public void testSyncApplication() throws IOException, Exception
  {
    SynchronousFeedApplication app = new SynchronousFeedApplication();
    if (app == null) {
      Assert.fail("could not get reference to the application");
    }
    StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }
}
