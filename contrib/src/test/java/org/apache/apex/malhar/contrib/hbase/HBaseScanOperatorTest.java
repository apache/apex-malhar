/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.hbase;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

/**
 *
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
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();

      dag.setAttribute(DAG.APPLICATION_NAME, "HBaseScanOperatorTest");
      TestHBaseScanOperator thop = dag.addOperator("testhbasescan", TestHBaseScanOperator.class);
      HBaseTupleCollector tc = dag.addOperator("tuplecollector", HBaseTupleCollector.class);
      dag.addStream("ss", thop.outputPort, tc.inputPort);

      thop.getStore().setTableName("table1");
      thop.getStore().setZookeeperQuorum("127.0.0.1");
      thop.getStore().setZookeeperClientPort(2181);

      LocalMode.Controller lc = lma.getController();
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
      List<HBaseTuple> tuples = HBaseTupleCollector.tuples;
      Assert.assertTrue(tuples.size() > 0);
      HBaseTuple tuple = HBaseTestHelper.findTuple(tuples, "row0", "colfam0", "col-0");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row0");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(), "col-0");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-0-0");
      tuple = HBaseTestHelper.findTuple(tuples, "row499", "colfam0", "col-0");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row499");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(), "col-0");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-499-0");
    } catch (Exception ex) {
      logger.error(ex.getMessage());
      assert false;
    }
  }

  public static class TestHBaseScanOperator extends HBaseScanOperator<HBaseTuple>
  {

    @Override
    public Scan operationScan()
    {
      Scan scan = new Scan();
      Filter f = new ColumnPrefixFilter(Bytes.toBytes("col-0"));
      scan.setFilter(f);
      return scan;
    }

    @Override
    //protected HBaseTuple getTuple(KeyValue[] kvs)
    protected HBaseTuple getTuple(Result result)
    {
      return HBaseTestHelper.getHBaseTuple(result);
    }

  }

}
