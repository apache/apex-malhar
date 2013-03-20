/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.contrib.hbase.HBaseScanOperatorTest.TestHBaseScanOperator;
import com.malhartech.stram.StramLocalCluster;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBasePutOperatorTest
{
private static final Logger logger = LoggerFactory.getLogger(HBaseScanOperatorTest.class);

  public HBasePutOperatorTest()
  {
  }

  @Test
  public void testPut()
  {
    try {
      HBaseTestHelper.clearHBase();
      DAG dag = new DAG();
      HBaseTupleGenerator tg = dag.addOperator("tuplegenerator", HBaseTupleGenerator.class);
      TestHBasePutOperator thop = dag.addOperator("testhbaseput", TestHBasePutOperator.class);
      dag.addStream("ss", tg.outputPort, thop.inputPort);

      thop.setTableName("table1");
      thop.setZookeeperQuorum("127.0.0.1");
      thop.setZookeeperClientPort(2822);

      StramLocalCluster lc = new StramLocalCluster(dag);
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(10000);
      /*
      tuples = new ArrayList<HBaseTuple>();
      TestHBaseScanOperator thop = new TestHBaseScanOperator();
           thop.setTableName("table1");
      thop.setZookeeperQuorum("127.0.0.1");
      thop.setZookeeperClientPort(2822);
      thop.setupConfiguration();

      thop.emitTuples();
      */

      // TODO review the generated test code and remove the default call to fail.
      //fail("The test case is a prototype.");
      // Check total number
      HBaseTuple tuple = HBaseTestHelper.getHBaseTuple("row0", "cf1");
      assert tuple.getRow().equals("row0");
      assert tuple.getColFamily().equals("cf1");
      assert tuple.getCol1Value().equals("val0-1");
      assert tuple.getCol2Value().equals("val0-2");
      tuple = HBaseTestHelper.getHBaseTuple("row499", "cf1");
      assert tuple.getRow().equals("row499");
      assert tuple.getColFamily().equals("cf1");
      assert tuple.getCol1Value().equals("val499-1");
      assert tuple.getCol2Value().equals("val499-2");
    } catch (Exception ex) {
      logger.error(ex.getMessage());
      assert false;
    }
  }

  public static class TestHBasePutOperator extends HBasePutOperator<HBaseTuple> {

    @Override
    public Put operationPut(HBaseTuple t)
    {
      Put put = new Put(t.getRow().getBytes());
      put.add(t.getColFamily().getBytes(), HBaseTestHelper.col1_bytes, t.getCol1Value().getBytes());
      put.add(t.getColFamily().getBytes(), HBaseTestHelper.col2_bytes, t.getCol2Value().getBytes());
      return put;
    }

  }
}
