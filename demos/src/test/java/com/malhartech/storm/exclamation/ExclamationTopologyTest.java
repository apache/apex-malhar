/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.storm.exclamation;

import com.malhartech.storm.exclamation.ExclamationTopology;
import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ExclamationTopologyTest
{
  public ExclamationTopologyTest()
  {
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    ExclamationTopology topology = new ExclamationTopology();
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
