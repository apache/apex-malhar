/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi;

import com.malhartech.demos.pi.PiCompareApplication;
import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class PiCompareApplicationTest
{
  @Test
  public void testSomeMethod() throws Exception
  {
    PiCompareApplication topology = new PiCompareApplication();
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

    }/*.start()*/;

    lc.run();
  }

}
