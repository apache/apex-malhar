/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import junit.framework.Assert;

import org.apache.hadoop.hbase.client.Append;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.DAG;
import com.malhartech.api.LocalMode;

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
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();

      dag.setAttribute(DAG.STRAM_APPNAME, "HBaseAppendOperatorTest");
      HBaseColTupleGenerator ctg = dag.addOperator("coltuplegenerator", HBaseColTupleGenerator.class);
      TestHBaseAppendOperator thop = dag.addOperator("testhbaseput", TestHBaseAppendOperator.class);
      dag.addStream("ss", ctg.outputPort, thop.inputPort);

      thop.setTableName("table1");
      thop.setZookeeperQuorum("127.0.0.1");
      thop.setZookeeperClientPort(2181);

      final LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(30000);

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
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row0");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(), "col-0");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-0-0");
      tuple = HBaseTestHelper.getHBaseTuple("row0", "colfam0","col-499");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row0");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(), "col-499");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-0-499");
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

    @Override
    public HBaseStatePersistenceStrategy getPersistenceStrategy() {
      return new HBaseRowStatePersistence();
    }

  }
}
