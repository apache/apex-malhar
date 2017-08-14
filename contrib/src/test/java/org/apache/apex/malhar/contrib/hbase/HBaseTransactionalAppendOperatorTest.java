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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Append;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

/**
 * Test for transactional append operator
 */
public class HBaseTransactionalAppendOperatorTest
{
  private static final Logger logger = LoggerFactory
      .getLogger(HBaseTransactionalAppendOperatorTest.class);

  public HBaseTransactionalAppendOperatorTest()
  {
  }

  @Test
  public void testAppend()
  {
    try {
      HBaseTestHelper.startLocalCluster();
      HBaseTestHelper.clearHBase();
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();

      dag.setAttribute(DAG.APPLICATION_NAME, "HBaseAppendOperatorTest");
      HBaseColTupleGenerator ctg = dag.addOperator("coltuplegenerator",
          HBaseColTupleGenerator.class);
      TestHBaseAppendOperator thop = dag.addOperator("testhbaseput",
          TestHBaseAppendOperator.class);
      dag.addStream("ss", ctg.outputPort, thop.input);

      thop.getStore().setTableName("table1");
      thop.getStore().setZookeeperQuorum("127.0.0.1");
      thop.getStore().setZookeeperClientPort(2181);

      final LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(30000);

      HBaseTuple tuple = HBaseTestHelper.getHBaseTuple("row0", "colfam0", "col-0");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row0");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(), "col-0");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-0-0");
      tuple = HBaseTestHelper.getHBaseTuple("row0", "colfam0", "col-499");
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

  public static class TestHBaseAppendOperator extends AbstractHBaseWindowAppendOutputOperator<HBaseTuple>
  {

    @Override
    public Append operationAppend(HBaseTuple t)
    {
      Append append = new Append(t.getRow().getBytes());
      append.add(t.getColFamily().getBytes(), t.getColName().getBytes(), t.getColValue().getBytes());
      return append;
    }

  }
}
