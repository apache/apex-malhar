/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import com.malhartech.api.DAG;
import com.malhartech.stram.StramLocalCluster;
import java.util.List;
import junit.framework.Assert;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBaseGetOperatorTest
{
  //private static final Logger logger = LoggerFactory.getLogger(HBaseGetOperatorTest.class);

  public HBaseGetOperatorTest()
  {
  }

  @Test
  public void testGet()
  {
    try {
      //HBaseTestHelper.startLocalCluster();
      HBaseTestHelper.populateHBase();
      DAG dag = new DAG();
      TestHBaseGetOperator thop = dag.addOperator("testhbaseget", TestHBaseGetOperator.class);
      HBaseTupleCollector tc = dag.addOperator("tuplecollector", HBaseTupleCollector.class);
      dag.addStream("ss", thop.outputPort, tc.inputPort);

      thop.setTableName("table1");
      thop.setZookeeperQuorum("127.0.0.1");
      thop.setZookeeperClientPort(2181);

      StramLocalCluster lc = new StramLocalCluster(dag);
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(30000);
      /*
      tuples = new ArrayList<HBaseTuple>();
      TestHBaseGetOperator thop = new TestHBaseGetOperator();
           thop.setTableName("table1");
      thop.setZookeeperQuorum("127.0.0.1");
      thop.setZookeeperClientPort(2822);
      thop.setupConfiguration();

      thop.emitTuples();
      */

      // TODO review the generated test code and remove the default call to fail.
      //fail("The test case is a prototype.");
      // Check total number
      List<HBaseTuple> tuples = HBaseTupleCollector.tuples;
      Assert.assertTrue( tuples.size() > 0);
      HBaseTuple tuple = HBaseTestHelper.findTuple(tuples, "row0", "colfam0", "col-0");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row0");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(), "col-0");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-0-0");
      Assert.assertTrue(tuples.size() >= 499);
      tuple = HBaseTestHelper.findTuple(tuples, "row0", "colfam0", "col-499");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row0");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(), "col-499");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-0-499");
    } catch (Exception ex) {
      ex.printStackTrace();
      //logger.error(ex.getMessage());
      assert false;
    }
  }

  public static class TestHBaseGetOperator extends HBaseGetOperator<HBaseTuple>
  {

    @Override
    public Get operationGet()
    {
      Get get = new Get(Bytes.toBytes("row0"));
      get.addFamily(HBaseTestHelper.colfam0_bytes);
      return get;
    }

    @Override
    //protected HBaseTuple getTuple(KeyValue[] kvs)
    protected HBaseTuple getTuple(KeyValue kv)
    {
      return HBaseTestHelper.getHBaseTuple(kv);
    }

  }

}