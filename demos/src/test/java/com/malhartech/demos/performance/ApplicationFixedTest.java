/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.performance;

import com.malhartech.stram.StramLocalCluster;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationFixedTest
{
  @Test
  public void testApplication() throws IOException, Exception
  {
    ApplicationFixed app = new ApplicationFixed();
    final StramLocalCluster lc = new StramLocalCluster(app.getApplication(new Configuration(false)));
    lc.run(60000);
  }
}
