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
public class HBaseScanOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(HBaseScanOperatorTest.class);

  public HBaseScanOperatorTest()
  {
  }

  @Test
  public void testScan()
  {
    try {
      HBaseTestHelper.populateHBase();
      DAG dag = new DAG();
      TestHBaseScanOperator thop = dag.addOperator("testhbasescan", TestHBaseScanOperator.class);
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
      List<HBaseTuple> tuples = HBaseTupleCollector.tuples;
      assert tuples.size() > 0;
      assert tuples.get(0).getCol1Value().equals("val0-1");
      assert tuples.get(0).getCol2Value().equals("val0-2");
      assert tuples.size() >= 499;
      boolean found = false;
      for (int i = 0; i < 500; ++i) {
        if (tuples.get(i).getRow().equals("row499")
                && tuples.get(i).getColFamily().equals("cf1")
                && tuples.get(i).getCol1Value().equals("val499-1")
                && tuples.get(i).getCol2Value().equals("val499-2")) {
          found = true;
          break;
        }
      }
      assert found;
    } catch (Exception ex) {
      logger.error(ex.getMessage());
      assert false;
    }
  }

  public static class TestHBaseScanOperator extends HBaseScanOperator<HBaseTuple>
  {
    private int rowIndex = 0;

    @Override
    public Scan operationScan()
    {
      return new Scan();
    }

    @Override
    //protected HBaseTuple getTuple(KeyValue[] kvs)
    protected HBaseTuple getTuple(Result result)
    {
      return HBaseTestHelper.getHBaseTuple(result);
    }

  }

}
