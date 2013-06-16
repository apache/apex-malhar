/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.twitter;

import com.datatorrent.demos.twitter.TwitterSampleInput;
import com.datatorrent.demos.twitter.TwitterTopCounterApplication;
import com.malhartech.api.LocalMode;
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

  /**
   * This test requires twitter authentication setup and is skipped by default
   * (see {@link TwitterSampleInput}).
   *
   * @throws Exception
   */
  @Ignore
  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    new TwitterTopCounterApplication().populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);
  }
}
