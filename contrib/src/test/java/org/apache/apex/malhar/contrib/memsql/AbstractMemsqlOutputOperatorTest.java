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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.jdbc.JdbcTransactionalStore;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.ProcessingMode;

import static org.apache.apex.malhar.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.APP_ID;
import static org.apache.apex.malhar.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.OPERATOR_ID;
import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class AbstractMemsqlOutputOperatorTest
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMemsqlOutputOperatorTest.class);

  public static final String HOST_PREFIX = "jdbc:mysql://";
  public static final String HOST = "localhost";
  public static final String USER = "root";
  public static final String PORT = "3307";
  public static final String DATABASE = "bench";
  public static final String TABLE = "bench";
  public static final String FQ_TABLE = DATABASE + "." + TABLE;
  public static final String INDEX_COLUMN = "data_index";
  public static final String DATA_COLUMN1 = "data1";
  public static final String DATA_COLUMN2 = "data2";
  public static final int NUM_WINDOWS = 10;
  public static final int BLAST_SIZE = 10;
  public static final int DATABASE_SIZE = NUM_WINDOWS; //* BLAST_SIZE;
  public static final int BATCH_SIZE = DATABASE_SIZE / 5;

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
        + "(" + DATA_COLUMN1
        + " INTEGER PRIMARY KEY, "
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

  public static void cleanDatabase() throws SQLException
  {
    memsqlInitializeDatabase(createStore(null, false));
  }

  @Test
  public void testMemsqlOutputOperator() throws Exception
  {
    cleanDatabase();
    MemsqlStore memsqlStore = createStore(null, true);

    MemsqlPOJOOutputOperator outputOperator = new MemsqlPOJOOutputOperator();
    outputOperator.setStore(memsqlStore);
    outputOperator.setBatchSize(BATCH_SIZE);
    outputOperator.setTablename(FQ_TABLE);
    ArrayList<String> columns = new ArrayList<String>();
    columns.add(DATA_COLUMN1);
    columns.add(DATA_COLUMN2);
    outputOperator.setDataColumns(columns);
    ArrayList<String> expressions = new ArrayList<String>();
    expressions.add("getIntVal()");
    expressions.add("getStringVal()");
    outputOperator.setExpression(expressions);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    outputOperator.setup(context);

    for (int wid = 0; wid < NUM_WINDOWS; wid++) {
      outputOperator.beginWindow(wid);
      innerObj.setIntVal(wid + 1);
      outputOperator.input.put(innerObj);

      outputOperator.endWindow();
    }

    outputOperator.teardown();

    memsqlStore.connect();

    int databaseSize;

    Statement statement = memsqlStore.getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery("select count(*) from " + FQ_TABLE);
    resultSet.next();
    databaseSize = resultSet.getInt(1);

    memsqlStore.disconnect();

    Assert.assertEquals("Numer of tuples in database", DATABASE_SIZE, databaseSize);
  }

  public InnerObj innerObj = new InnerObj();

  /**
   * @return the innerObj
   */
  public InnerObj getInnerObj()
  {
    return innerObj;
  }

  /**
   * @param innerObj the innerObj to set
   */
  public void setInnerObj(InnerObj innerObj)
  {
    this.innerObj = innerObj;
  }

  public class InnerObj
  {
    public InnerObj()
    {
    }

    private int intVal = 11;
    private String stringVal = "hello";

    /**
     * @return the intVal
     */
    public int getIntVal()
    {
      return intVal;
    }

    /**
     * @param intVal the intVal to set
     */
    public void setIntVal(int intVal)
    {
      this.intVal = intVal;
    }

    /**
     * @return the stringVal
     */
    public String getStringVal()
    {
      return stringVal;
    }

    /**
     * @param stringVal the stringVal to set
     */
    public void setStringVal(String stringVal)
    {
      this.stringVal = stringVal;
    }

  }

}
