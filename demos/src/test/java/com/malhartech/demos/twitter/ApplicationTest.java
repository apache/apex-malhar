/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.stram.DAGPropertiesBuilder;
import com.malhartech.stram.StramLocalCluster;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
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
    testApplication(false);
  }

  @Test
  public void testSyncApplication() throws IOException, Exception
  {
    testApplication(true);
  }

  public void testApplication(boolean sync) throws Exception
  {
    StramLocalCluster lc = new StramLocalCluster(new TwitterTopCounter(new Configuration(false), sync));
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }
}
