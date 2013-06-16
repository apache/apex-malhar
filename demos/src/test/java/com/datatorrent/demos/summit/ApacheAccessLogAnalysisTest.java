/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.demos.summit;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.malhartech.api.LocalMode;
/**
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class ApacheAccessLogAnalysisTest
{
  @Test
  public void testApplication() throws Exception
  {
  	//LocalMode.runApp(new ApacheAccessLogAnalaysis(), 10000);
  	ApacheAccessLogAnalaysis app = new ApacheAccessLogAnalaysis();
    LocalMode lma = LocalMode.newInstance();
    app.populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();
  }
}
