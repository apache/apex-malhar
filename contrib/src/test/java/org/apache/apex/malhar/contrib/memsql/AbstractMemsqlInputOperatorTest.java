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
package org.apache.apex.malhar.contrib.memsql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.jdbc.JdbcTransactionalStore;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class AbstractMemsqlInputOperatorTest
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMemsqlInputOperatorTest.class);

  public static final String HOST_PREFIX = "jdbc:mysql://";
  public static final String HOST = "localhost";
  public static final String USER = "root";
  public static final String PORT = "3307";
  public static final String DATABASE = "bench";
  public static final String TABLE = "testtable";
  public static final String FQ_TABLE = DATABASE + "." + TABLE;
  public static final String INDEX_COLUMN = "data_index";
  public static final String DATA_COLUMN2 = "data2";
  public static final int BLAST_SIZE = 10;
  public static final int NUM_WINDOWS = 10;
  public static final int DATABASE_SIZE = NUM_WINDOWS * BLAST_SIZE;
  public static final int OPERATOR_ID = 0;

  public static void populateDatabase(MemsqlStore memsqlStore)
  {
    memsqlStore.connect();

    try {
      String insert = "insert into " + FQ_TABLE + " (" + INDEX_COLUMN + "," + DATA_COLUMN2 + ") " + "VALUES (" + "?,?" + ")";
      PreparedStatement stmt = memsqlStore.getConnection().prepareStatement(insert);
      for (int counter = 0; counter < DATABASE_SIZE; counter++) {
        String test = "Testname" + counter;
        stmt.setInt(1, counter);
        stmt.setString(2, test);
        stmt.executeUpdate();
      }

      stmt.close();
    } catch (SQLException ex) {
      LOG.error(null, ex);
    }

    memsqlStore.disconnect();
  }

  public static void memsqlInitializeDatabase(MemsqlStore memsqlStore) throws SQLException
  {
    memsqlStore.connect();

    Statement statement = memsqlStore.getConnection().createStatement();
    statement.executeUpdate("drop database if exists " + DATABASE);
    statement.executeUpdate("create database " + DATABASE);

    memsqlStore.disconnect();

    memsqlStore.connect();

    statement = memsqlStore.getConnection().createStatement();
    statement.executeUpdate("create table "
        + FQ_TABLE
        + "(" + INDEX_COLUMN + " INTEGER PRIMARY KEY, "
        + DATA_COLUMN2
        + " VARCHAR(256))");
    String createMetaTable = "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + JdbcTransactionalStore.DEFAULT_META_TABLE + " ( "
        + JdbcTransactionalStore.DEFAULT_APP_ID_COL + " VARCHAR(100) NOT NULL, "
        + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT NOT NULL, "
        + JdbcTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT NOT NULL, "
        + "PRIMARY KEY (" + JdbcTransactionalStore.DEFAULT_APP_ID_COL + ", " + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + ") "
        + ")";

    statement.executeUpdate(createMetaTable);

    statement.close();

    memsqlStore.disconnect();
  }

  public static MemsqlStore createStore(MemsqlStore memsqlStore, boolean withDatabase)
  {
    String host = HOST;
    String user = USER;
    String port = PORT;

    if (memsqlStore == null) {
      memsqlStore = new MemsqlStore();
    }

    StringBuilder sb = new StringBuilder();
    String tempHost = HOST_PREFIX + host + ":" + PORT;
    if (withDatabase) {
      tempHost += "/" + DATABASE;
    }
    LOG.debug("Host name: {}", tempHost);
    LOG.debug("User name: {}", user);
    LOG.debug("Port: {}", port);
    memsqlStore.setDatabaseUrl(tempHost);

    sb.append("user:").append(user).append(",");
    sb.append("port:").append(port);

    String properties = sb.toString();
    LOG.debug(properties);
    memsqlStore.setConnectionProperties(properties);
    return memsqlStore;
  }

  public static void cleanDatabase() throws SQLException
  {
    memsqlInitializeDatabase(createStore(null, false));
  }

  @Test
  public void TestMemsqlInputOperator() throws SQLException
  {
    cleanDatabase();
    populateDatabase(createStore(null, true));

    MemsqlInputOperator inputOperator = new MemsqlInputOperator();
    createStore((MemsqlStore)inputOperator.getStore(), true);
    inputOperator.setBlastSize(BLAST_SIZE);
    inputOperator.setTablename(FQ_TABLE);
    inputOperator.setPrimaryKeyCol(INDEX_COLUMN);
    inputOperator.setTablename(FQ_TABLE);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(null);

    for (int wid = 0; wid < NUM_WINDOWS + 1; wid++) {
      inputOperator.beginWindow(wid);
      inputOperator.emitTuples();
      inputOperator.endWindow();
    }

    Assert.assertEquals("Number of tuples in database", DATABASE_SIZE, sink.collectedTuples.size());
  }

  /*
   * This test can be run against memsql installation on node17.
   */
  @Test
  public void TestMemsqlPOJOInputOperator() throws SQLException
  {
    cleanDatabase();
    populateDatabase(createStore(null, true));
    Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(Context.OperatorContext.SPIN_MILLIS, 500);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);
    MemsqlPOJOInputOperator inputOperator = new MemsqlPOJOInputOperator();
    createStore((MemsqlStore)inputOperator.getStore(), true);
    inputOperator.setBatchSize(10);
    inputOperator.setTablename(FQ_TABLE);
    inputOperator.setPrimaryKeyColumn(INDEX_COLUMN);
    ArrayList<String> expressions = new ArrayList<String>();
    expressions.add("id");
    expressions.add("name");
    inputOperator.setExpressions(expressions);
    ArrayList<String> columns = new ArrayList<String>();
    columns.add("data_index");
    columns.add("data2");
    inputOperator.setColumns(columns);
    inputOperator.setQuery("select * from " + FQ_TABLE + ";");
    inputOperator.setOutputClass("org.apache.apex.malhar.contrib.memsql.TestInputPojo");
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);

    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("rows from db", 100, sink.collectedTuples.size());
    for (int i = 0; i < 10; i++) {
      TestInputPojo object = (TestInputPojo)sink.collectedTuples.get(i);
      Assert.assertEquals("id set in testpojo", i, object.getId());
      Assert.assertEquals("name set in testpojo", "Testname" + i, object.getName());
    }
    sink.clear();
    inputOperator.setQuery("select * from " + FQ_TABLE + " where " + "%p " + ">= " + "%s" + ";");
    inputOperator.setStartRow(10);
    inputOperator.setup(context);

    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();
    Assert.assertEquals("rows from db", 90, sink.collectedTuples.size());
    sink.clear();
    inputOperator.setQuery("select * from " + FQ_TABLE + " where " + "%p " + ">= " + "%s" + " LIMIT " + "%l" + ";");
    inputOperator.setStartRow(1);

    inputOperator.setBatchSize(10);
    inputOperator.setup(context);

    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();
    Assert.assertEquals("rows from db", 10, sink.collectedTuples.size());

  }

}
