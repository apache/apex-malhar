/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import com.malhartech.api.DAG;
import com.malhartech.stram.StramLocalCluster;
import java.util.List;
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
  private static final Logger logger = LoggerFactory.getLogger(HBaseGetOperatorTest.class);

  public HBaseGetOperatorTest()
  {
  }

  @Test
  public void testGet()
  {
    try {
      HBaseTestHelper.populateHBase();
      DAG dag = new DAG();
      TestHBaseGetOperator thop = dag.addOperator("testhbaseget", TestHBaseGetOperator.class);
      HBaseTupleCollector tc = dag.addOperator("tuplecollector", HBaseTupleCollector.class);
      dag.addStream("ss", thop.outputPort, tc.inputPort);

      thop.setTableName("table1");
      thop.setZookeeperQuorum("127.0.0.1");
      thop.setZookeeperClientPort(2822);

      StramLocalCluster lc = new StramLocalCluster(dag);
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(10000);
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
      assert tuples.size() > 0;
      assert tuples.get(0).getRow().equals("row0");
      assert tuples.get(0).getColFamily().equals("cf1");
      assert tuples.get(0).getCol1Value().equals("val0-1");
      assert tuples.get(0).getCol2Value().equals("val0-2");
      assert tuples.size() >= 499;
      assert tuples.get(499).getRow().equals("row499");
      assert tuples.get(499).getColFamily().equals("cf1");
      assert tuples.get(499).getCol1Value().equals("val499-1");
      assert tuples.get(499).getCol2Value().equals("val499-2");
    } catch (Exception ex) {
      logger.error(ex.getMessage());
      assert false;
    }
  }

  public static class TestHBaseGetOperator extends HBaseGetOperator<HBaseTuple>
  {
    private int rowIndex = 0;

    @Override
    public Get operationGet()
    {
      Get get = new Get(Bytes.toBytes("row" + rowIndex++));
      get.addFamily(HBaseTestHelper.cf1_bytes);
      if (rowIndex >= 500) rowIndex = 0;
      return get;
    }

    @Override
    //protected HBaseTuple getTuple(KeyValue[] kvs)
    protected HBaseTuple getTuple(Result result)
    {
      return HBaseTestHelper.getHBaseTuple(result);
    }

  }

}