/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.samplestream;

import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class ApplicationTest
{
  public ApplicationTest()
  {
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    Application topology = new Application();
    final StramLocalCluster lc = new StramLocalCluster(topology.getApplication(new Configuration(false)));

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(10000);
        }
        catch (InterruptedException ex) {
        }

        lc.shutdown();
      }
    }.start();

    lc.run();
  }
}
