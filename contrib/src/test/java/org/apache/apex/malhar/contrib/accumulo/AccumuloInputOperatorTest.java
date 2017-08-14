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
package org.apache.apex.malhar.contrib.accumulo;

import java.util.List;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

import com.datatorrent.netlet.util.DTThrowable;

public class AccumuloInputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(AccumuloInputOperatorTest.class);

  @Test
  public void testScan()
  {
    try {
      AccumuloTestHelper.getConnector();
      AccumuloTestHelper.deleteTable();
      AccumuloTestHelper.createTable();

      AccumuloTestHelper.populateAccumulo();
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();

      dag.setAttribute(DAG.APPLICATION_NAME, "AccumuloInputTest");
      TestAccumuloInputOperator taip = dag.addOperator("testaccumuloinput", TestAccumuloInputOperator.class);
      AccumuloTupleCollector tc = dag.addOperator("tuplecollector",AccumuloTupleCollector.class);
      dag.addStream("ss", taip.outputPort, tc.inputPort);

      taip.getStore().setTableName("tab1");
      taip.getStore().setZookeeperHost("127.0.0.1");
      taip.getStore().setInstanceName("instance");
      taip.getStore().setUserName("root");
      taip.getStore().setPassword("pass");
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(30000);

      List<AccumuloTuple> tuples = AccumuloTupleCollector.tuples;
      Assert.assertTrue(tuples.size() > 0);
      AccumuloTuple tuple = AccumuloTestHelper.findTuple(tuples, "row0","colfam0", "col-0");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row0");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(),"colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(),"col-0");
      Assert.assertEquals("Tuple column value", tuple.getColValue(),"val-0-0");
      tuple = AccumuloTestHelper.findTuple(tuples, "row499", "colfam0","col-0");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row499");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(),"colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(),"col-0");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-499-0");
    } catch (Exception ex) {
      logger.error(ex.getMessage());
      assert false;
    }
  }

  public static class TestAccumuloInputOperator extends AbstractAccumuloInputOperator<AccumuloTuple>
  {

    @Override
    public AccumuloTuple getTuple(Entry<Key, Value> entry)
    {
      AccumuloTuple tuple = new AccumuloTuple();
      tuple.setRow(entry.getKey().getRow().toString());
      tuple.setColFamily(entry.getKey().getColumnFamily().toString());
      tuple.setColName(entry.getKey().getColumnQualifier().toString());
      tuple.setColValue(entry.getValue().toString());
      return tuple;
    }

    @Override
    public Scanner getScanner(Connector conn)
    {
      Authorizations auths = new Authorizations();
      Scanner scan = null;
      try {
        scan = conn.createScanner(getStore().getTableName(), auths);
      } catch (TableNotFoundException e) {
        logger.error("table not found ");
        DTThrowable.rethrow(e);
      }
      scan.setRange(new Range());
      // scan.fetchColumnFamily("attributes");

      return scan;
    }

  }
}
