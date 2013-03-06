/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi;

import com.malhartech.api.DAG;
import com.malhartech.demos.pi.*;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.StramLocalCluster;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class TupleRecorderTest
{
private static File testWorkDir = new File("target", "TupleRecorderTest");

  @Test
  public void testTupleRecorder() throws Exception
  {
    ApplicationWithScript topology = new ApplicationWithScript();
    DAG dag = topology.getApplication(new Configuration(false));
    dag.getAttributes().attr(DAG.STRAM_APP_PATH).set("file://" + testWorkDir.getAbsolutePath());
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

    PTOperator ptOp = lc.findByLogicalNode(dag.getOperatorMeta("picalc"));
    Thread.sleep(5000);

    lc.getStreamingContainerManager().startRecording(ptOp.getId(), "doesNotMatter");

    Thread.sleep(10000);
    lc.getStreamingContainerManager().stopRecording(ptOp.getId());
    Thread.sleep(5000);
    lc.shutdown();
  }
}