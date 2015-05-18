/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.contrib.memsql;

import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractMemsqlInputOperatorTest
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMemsqlInputOperatorTest.class);

  public static final String HOST_PREFIX = "jdbc:mysql://";
  public static final String HOST = "localhost";
  public static final String USER = "root";
  public static final String PORT = "3307";
  public static final String DATABASE = "bench";
  public static final String TABLE = "bench";
  public static final String FQ_TABLE = DATABASE + "." + TABLE;
  public static final String INDEX_COLUMN = "data_index";
  public static final String DATA_COLUMN1 = "data1";
  public static final int BLAST_SIZE = 10;
  public static final int NUM_WINDOWS = 10;
  public static final int DATABASE_SIZE = NUM_WINDOWS * BLAST_SIZE;

  public static void populateDatabase(MemsqlStore memsqlStore)
  {
    memsqlStore.connect();

    try {
      Random random = new Random();
      Statement statement = memsqlStore.getConnection().createStatement();

      for(int counter = 0;
          counter < DATABASE_SIZE;
          counter++) {
        statement.executeUpdate("insert into " +
                                FQ_TABLE +
                                " (" + DATA_COLUMN1 + ") values (" + random.nextInt() + ")");
      }

      statement.close();
    }
    catch (SQLException ex) {
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
    statement.executeUpdate("create table " +
                            FQ_TABLE +
                            "(" + INDEX_COLUMN +
                            " INTEGER AUTO_INCREMENT PRIMARY KEY, " +
                            DATA_COLUMN1 +
                            " INTEGER)");
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
    createStore((MemsqlStore) inputOperator.getStore(), true);
    inputOperator.setBlastSize(BLAST_SIZE);
    inputOperator.setTablename(FQ_TABLE);
    inputOperator.setPrimaryKeyCol(INDEX_COLUMN);

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(null);

    for(int wid = 0;
        wid < NUM_WINDOWS + 1;
        wid++) {
      inputOperator.beginWindow(wid);
      inputOperator.emitTuples();
      inputOperator.endWindow();
    }

    Assert.assertEquals("Number of tuples in database", DATABASE_SIZE, sink.collectedTuples.size());
  }
}
