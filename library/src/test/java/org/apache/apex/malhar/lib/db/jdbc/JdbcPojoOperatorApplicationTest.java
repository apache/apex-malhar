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

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;

public class JdbcPojoOperatorApplicationTest extends JdbcOperatorTest
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

  @Test
  public void testApplication() throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/JdbcProperties.xml"));
      lma.prepareDAG(new JdbcPojoOperatorApplication(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          return getNumOfRowsinTable(TABLE_POJO_NAME) == 10;
        }
      });
      lc.run(10000);// runs for 10 seconds and quits
      Assert.assertEquals("rows in db", 10, getNumOfRowsinTable(TABLE_POJO_NAME));
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  public static class JdbcPojoOperatorApplication implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
      JdbcPOJOInsertOutputOperator jdbc = dag.addOperator("JdbcOutput", new JdbcPOJOInsertOutputOperator());
      JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
      outputStore.setDatabaseDriver(DB_DRIVER);
      outputStore.setDatabaseUrl(URL);
      jdbc.setStore(outputStore);
      jdbc.setBatchSize(3);
      jdbc.setTablename(TABLE_POJO_NAME);
      dag.getMeta(jdbc).getMeta(jdbc.input).getAttributes().put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
      cleanTable();
      JdbcPojoEmitterOperator input = dag.addOperator("data", new JdbcPojoEmitterOperator());
      dag.addStream("pojo", input.output, jdbc.input);
    }
  }

  public static class JdbcPojoEmitterOperator extends BaseOperator implements InputOperator
  {
    public static int emitTuple = 10;
    public final transient DefaultOutputPort<TestPOJOEvent> output = new DefaultOutputPort<TestPOJOEvent>();

    @Override
    public void emitTuples()
    {
      if (emitTuple > 0) {
        output.emit(new TestPOJOEvent(emitTuple,"test" + emitTuple));
        emitTuple--;
        TupleCount++;
      }
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      TupleCount = 0;
    }
  }
}
