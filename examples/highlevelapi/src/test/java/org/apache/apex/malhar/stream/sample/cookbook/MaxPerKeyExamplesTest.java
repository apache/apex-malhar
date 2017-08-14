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
package org.apache.apex.malhar.stream.sample.cookbook;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.apex.malhar.lib.db.jdbc.JdbcTransactionalStore;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Throwables;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.StramLocalCluster;

/**
 * Test for MaxPerKeyExamples Application.
 */
public class MaxPerKeyExamplesTest
{

  private static final String INPUT_TUPLE_CLASS = "org.apache.apex.malhar.stream.sample.cookbook.InputPojo";
  private static final String OUTPUT_TUPLE_CLASS = "org.apache.apex.malhar.stream.sample.cookbook.OutputPojo";
  private static final String DB_DRIVER = "org.h2.Driver";
  private static final String DB_URL = "jdbc:h2:~/test";
  private static final String INPUT_TABLE = "InputTable";
  private static final String OUTPUT_TABLE = "OutputTable";
  private static final String USER_NAME = "root";
  private static final String PSW = "password";
  private static final String QUERY = "SELECT * FROM " + INPUT_TABLE + ";";

  private static final double[] MEANTEMPS = {85.3, 75.4};

  @BeforeClass
  public static void setup()
  {
    try {
      Class.forName(DB_DRIVER).newInstance();

      Connection con = DriverManager.getConnection(DB_URL,USER_NAME,PSW);
      Statement stmt = con.createStatement();

      String createMetaTable = "CREATE TABLE IF NOT EXISTS " + JdbcTransactionalStore.DEFAULT_META_TABLE + " ( "
          + JdbcTransactionalStore.DEFAULT_APP_ID_COL + " VARCHAR(100) NOT NULL, "
          + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT NOT NULL, "
          + JdbcTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT NOT NULL, "
          + "UNIQUE (" + JdbcTransactionalStore.DEFAULT_APP_ID_COL + ", "
          + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + ", " + JdbcTransactionalStore.DEFAULT_WINDOW_COL + ") "
          + ")";
      stmt.executeUpdate(createMetaTable);

      String createInputTable = "CREATE TABLE IF NOT EXISTS " + INPUT_TABLE
          + "(MONTH INT(2) not NULL, DAY INT(2), YEAR INT(4), MEANTEMP DOUBLE(10) )";
      stmt.executeUpdate(createInputTable);

      String createOutputTable = "CREATE TABLE IF NOT EXISTS " + OUTPUT_TABLE
          + "(MONTH INT(2) not NULL, MEANTEMP DOUBLE(10) )";
      stmt.executeUpdate(createOutputTable);

      String cleanTable = "truncate table " + INPUT_TABLE;
      stmt.executeUpdate(cleanTable);

      stmt = con.createStatement();

      String sql = "INSERT INTO " + INPUT_TABLE + " VALUES (6, 21, 2014, 85.3)";
      stmt.executeUpdate(sql);
      sql = "INSERT INTO " + INPUT_TABLE + " VALUES (7, 20, 2014, 75.4)";
      stmt.executeUpdate(sql);
      sql = "INSERT INTO " + INPUT_TABLE + " VALUES (6, 18, 2014, 45.3)";
      stmt.executeUpdate(sql);

    } catch (Throwable e) {
      throw Throwables.propagate(e);
    }
  }

  @AfterClass
  public static void cleanup()
  {
    try {
      Class.forName(DB_DRIVER).newInstance();

      Connection con = DriverManager.getConnection(DB_URL, USER_NAME, PSW);
      Statement stmt = con.createStatement();

      String dropInputTable = "DROP TABLE " + INPUT_TABLE;
      stmt.executeUpdate(dropInputTable);

      String dropOutputTable = "DROP TABLE " + OUTPUT_TABLE;
      stmt.executeUpdate(dropOutputTable);

    } catch (Throwable e) {
      throw Throwables.propagate(e);
    }

  }

  public void setConfig(Configuration conf)
  {
    conf.set("dt.operator.jdbcInput.prop.store.userName", USER_NAME);
    conf.set("dt.operator.jdbcInput.prop.store.password", PSW);
    conf.set("dt.operator.jdbcInput.prop.store.databaseDriver", DB_DRIVER);
    conf.set("dt.operator.jdbcInput.prop.batchSize", "5");
    conf.set("dt.operator.jdbcInput.port.outputPort.attr.TUPLE_CLASS", INPUT_TUPLE_CLASS);
    conf.set("dt.operator.jdbcInput.prop.store.databaseUrl", DB_URL);
    conf.set("dt.operator.jdbcInput.prop.tableName", INPUT_TABLE);
    conf.set("dt.operator.jdbcInput.prop.query", QUERY);

    conf.set("dt.operator.jdbcOutput.prop.store.userName", USER_NAME);
    conf.set("dt.operator.jdbcOutput.prop.store.password", PSW);
    conf.set("dt.operator.jdbcOutput.prop.store.databaseDriver", DB_DRIVER);
    conf.set("dt.operator.jdbcOutput.prop.batchSize", "5");
    conf.set("dt.operator.jdbcOutput.port.input.attr.TUPLE_CLASS", OUTPUT_TUPLE_CLASS);
    conf.set("dt.operator.jdbcOutput.prop.store.databaseUrl", DB_URL);
    conf.set("dt.operator.jdbcOutput.prop.tablename", OUTPUT_TABLE);
  }

  public int getNumEntries()
  {
    Connection con;
    try {
      con = DriverManager.getConnection(DB_URL,USER_NAME,PSW);
      Statement stmt = con.createStatement();

      String countQuery = "SELECT count(DISTINCT (MONTH, MEANTEMP)) from " + OUTPUT_TABLE;
      ResultSet resultSet = stmt.executeQuery(countQuery);
      resultSet.next();
      return resultSet.getInt(1);
    } catch (SQLException e) {
      throw new RuntimeException("fetching count", e);
    }
  }

  public Map<Integer, Double> getMaxMeanTemp()
  {
    Map<Integer, Double> result = new HashMap<>();
    Connection con;
    try {
      con = DriverManager.getConnection(DB_URL,USER_NAME,PSW);
      Statement stmt = con.createStatement();

      String countQuery = "SELECT DISTINCT * from " + OUTPUT_TABLE;
      ResultSet resultSet = stmt.executeQuery(countQuery);
      while (resultSet.next()) {
        result.put(resultSet.getInt("MONTH"), resultSet.getDouble("MEANTEMP"));

      }
      return result;
    } catch (SQLException e) {
      throw new RuntimeException("fetching count", e);
    }
  }

  @Test
  public void MaxPerKeyExampleTest() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    setConfig(conf);

    MaxPerKeyExamples app = new MaxPerKeyExamples();

    lma.prepareDAG(app, conf);

    LocalMode.Controller lc = lma.getController();
    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return getNumEntries() == 2;
      }
    });

    lc.run(5000);

    double[] result = new double[2];
    result[0] = getMaxMeanTemp().get(6);
    result[1] = getMaxMeanTemp().get(7);
    Assert.assertArrayEquals(MEANTEMPS, result, 0.0);
  }
}
