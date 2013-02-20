/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi2;

import com.malhartech.api.DAG;
import com.malhartech.demos.pi.*;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class ApplicationTest
{
  //@Test
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

    }/*.start()*/;

    lc.run();
  }

  @Test
  public void testTupleRecorder() throws Exception
  {
    Application topology = new Application();
    DAG dag = topology.getApplication(new Configuration(false));
    final StramLocalCluster lc = new StramLocalCluster(dag);

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

    lc.runAsync();

    PTOperator ptOp = lc.findByLogicalNode(dag.getOperatorWrapper("picalc"));
    Thread.sleep(5000);

    lc.getStreamingContainerManager().startRecording(ptOp.getId(), "doesNotMatter");

    Thread.sleep(10000);
    lc.getStreamingContainerManager().stopRecording(ptOp.getId());
    Thread.sleep(5000);
    lc.shutdown();
  }
}