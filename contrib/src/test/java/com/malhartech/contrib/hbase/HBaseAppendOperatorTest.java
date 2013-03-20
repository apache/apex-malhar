/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import com.malhartech.api.DAG;
import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBaseAppendOperatorTest
{
private static final Logger logger = LoggerFactory.getLogger(HBasePutOperatorTest.class);

  public HBaseAppendOperatorTest()
  {
  }

  @Test
  public void testAppend()
  {
    try {
      HBaseTestHelper.clearHBase();
      DAG dag = new DAG();
      HBaseColTupleGenerator ctg = dag.addOperator("coltuplegenerator", HBaseColTupleGenerator.class);
      TestHBaseAppendOperator thop = dag.addOperator("testhbaseput", TestHBaseAppendOperator.class);
      dag.addStream("ss", ctg.outputPort, thop.inputPort);

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
      HBaseTuple tuple = HBaseTestHelper.getHBaseTuple("row0", "colfam0", "col-0");
      assert tuple != null;
      assert tuple.getRow().equals("row0");
      assert tuple.getColFamily().equals("colfam0");
      assert tuple.getColName().equals("col-0");
      assert tuple.getColValue().equals("val-0-0");
      tuple = HBaseTestHelper.getHBaseTuple("row0", "colfam0","col-499");
      assert tuple != null;
      assert tuple.getRow().equals("row0");
      assert tuple.getColFamily().equals("colfam0");
      assert tuple.getColName().equals("col-499");
      assert tuple.getColValue().equals("val-0-499");
    } catch (Exception ex) {
      logger.error(ex.getMessage());
      assert false;
    }
  }

  public static class TestHBaseAppendOperator extends HBaseAppendOperator<HBaseTuple> {

    @Override
    public Append operationAppend(HBaseTuple t)
    {
      Append append = new Append(t.getRow().getBytes());
      append.add(t.getColFamily().getBytes(), t.getColName().getBytes(), t.getColValue().getBytes());
      return append;
    }

  }
}
