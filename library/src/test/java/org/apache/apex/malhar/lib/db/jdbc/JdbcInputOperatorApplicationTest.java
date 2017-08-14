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
package org.apache.apex.malhar.lib.db.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;

/**
 * Tests to check if setting List of {@link FieldInfo} externally from
 * configuration file works fine for Jdbc input operators.
 */
public class JdbcInputOperatorApplicationTest extends JdbcOperatorTest
{
  public static int TupleCount;

  public int getNumOfRowsinTable(String tableName)
  {
    Connection con;
    try {
      con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();
      String countQuery = "SELECT count(*) from " + tableName;
      ResultSet resultSet = stmt.executeQuery(countQuery);
      resultSet.next();
      return resultSet.getInt(1);
    } catch (SQLException e) {
      throw new RuntimeException("fetching count", e);
    }
  }

  public void testApplication(StreamingApplication streamingApplication) throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/JdbcProperties.xml"));
      lma.prepareDAG(streamingApplication, conf);
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          return TupleCount == 10;
        }
      });
      lc.run(10000);// runs for 10 seconds and quits
      Assert.assertEquals("rows in db", TupleCount, getNumOfRowsinTable(TABLE_POJO_NAME));
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }

  }

  @Test
  public void testJdbcPOJOPollInputOperatorApplication() throws Exception
  {
    testApplication(new JdbcPOJOPollInputOperatorApplication());
  }

  @Test
  public void testJdbcPOJOInputOperatorApplication() throws Exception
  {
    testApplication(new JdbcPOJOInputOperatorApplication());
  }


  public static class JdbcPOJOInputOperatorApplication implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
      cleanTable();
      insertEvents(10, true, 0);
      JdbcPOJOInputOperator inputOperator = dag.addOperator("JdbcPOJOInput", new JdbcPOJOInputOperator());
      JdbcStore store = new JdbcStore();
      store.setDatabaseDriver(DB_DRIVER);
      store.setDatabaseUrl(URL);
      inputOperator.setStore(store);
      inputOperator.setTableName(TABLE_POJO_NAME);
      inputOperator.setFetchSize(100);
      dag.getMeta(inputOperator).getMeta(inputOperator.outputPort).getAttributes().put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
      ResultCollector result = dag.addOperator("result", new ResultCollector());
      dag.addStream("pojo", inputOperator.outputPort, result.input);
    }
  }

  public static class JdbcPOJOPollInputOperatorApplication implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
      cleanTable();
      insertEvents(10, true, 0);
      JdbcPOJOPollInputOperator inputOperator = dag.addOperator("JdbcPOJOPollInput", new JdbcPOJOPollInputOperator());
      JdbcStore store = new JdbcStore();
      store.setDatabaseDriver(DB_DRIVER);
      store.setDatabaseUrl(URL);
      inputOperator.setStore(store);
      inputOperator.setTableName(TABLE_POJO_NAME);
      inputOperator.setKey("id");
      inputOperator.setFetchSize(100);
      inputOperator.setBatchSize(100);
      inputOperator.setPartitionCount(2);
      dag.getMeta(inputOperator).getMeta(inputOperator.outputPort).getAttributes().put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
      ResultCollector result = dag.addOperator("result", new ResultCollector());
      dag.addStream("pojo", inputOperator.outputPort, result.input);
    }
  }

  public static class ResultCollector extends BaseOperator
  {
    public final transient DefaultInputPort<java.lang.Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(java.lang.Object in)
      {
        TestPOJOEvent obj = (TestPOJOEvent)in;
        TupleCount++;
      }
    };

    @Override
    public void setup(Context.OperatorContext context)
    {
      TupleCount = 0;
    }
  }

}
