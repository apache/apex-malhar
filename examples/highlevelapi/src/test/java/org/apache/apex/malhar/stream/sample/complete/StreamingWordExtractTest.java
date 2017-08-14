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
package org.apache.apex.malhar.stream.sample.complete;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.apex.malhar.lib.db.jdbc.JdbcTransactionalStore;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Throwables;
import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.StramLocalCluster;

/**
 * Testing StreamingWordExtract application
 */
public class StreamingWordExtractTest
{
  private static final String TUPLE_CLASS = "org.apache.apex.malhar.stream.sample.complete.PojoEvent";
  private static final String DB_DRIVER = "org.h2.Driver";
  private static final String DB_URL = "jdbc:h2:~/test";
  private static final String TABLE_NAME = "Test";
  private static final String USER_NAME = "root";
  private static final String PSW = "password";

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

      String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME
          + "(STRINGVALUE VARCHAR(255))";
      stmt.executeUpdate(createTable);

    } catch (Throwable e) {
      throw Throwables.propagate(e);
    }
  }

  @After
  public void cleanTable()
  {
    try {
      Connection con = DriverManager.getConnection(DB_URL,USER_NAME,PSW);
      Statement stmt = con.createStatement();
      String dropTable = "drop table " + TABLE_NAME;
      stmt.executeUpdate(dropTable);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void setConfig(Configuration conf)
  {
    conf.set("dt.operator.jdbcOutput.prop.store.userName", USER_NAME);
    conf.set("dt.operator.jdbcOutput.prop.store.password", PSW);
    conf.set("dt.operator.jdbcOutput.prop.store.databaseDriver", DB_DRIVER);
    conf.set("dt.operator.jdbcOutput.prop.batchSize", "5");
    conf.set("dt.operator.jdbcOutput.port.input.attr.TUPLE_CLASS", TUPLE_CLASS);
    conf.set("dt.operator.jdbcOutput.prop.store.databaseUrl", DB_URL);
    conf.set("dt.operator.jdbcOutput.prop.tablename", TABLE_NAME);
  }

  public int getNumOfEventsInStore()
  {
    Connection con;
    try {
      con = DriverManager.getConnection(DB_URL,USER_NAME,PSW);
      Statement stmt = con.createStatement();

      String countQuery = "SELECT count(*) from " + TABLE_NAME;
      ResultSet resultSet = stmt.executeQuery(countQuery);
      resultSet.next();
      return resultSet.getInt(1);
    } catch (SQLException e) {
      throw new RuntimeException("fetching count", e);
    }
  }

  @Test
  public void StreamingWordExtractTest() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    setConfig(conf);
    StreamingWordExtract app = new StreamingWordExtract();
    lma.prepareDAG(app, conf);
    LocalMode.Controller lc = lma.getController();

    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return getNumOfEventsInStore() == 36;
      }
    });

    lc.run(10000);

    Assert.assertEquals(app.getWordCount(), getNumOfEventsInStore());
    Assert.assertEquals(app.getEntriesMapped(), getNumOfEventsInStore());
  }

}
