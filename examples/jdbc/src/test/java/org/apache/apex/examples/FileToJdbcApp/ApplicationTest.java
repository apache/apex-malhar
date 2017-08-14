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
package org.apache.apex.examples.FileToJdbcApp;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.apex.malhar.lib.db.jdbc.JdbcTransactionalStore;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Test the DAG declaration in local mode.<br>
 * The assumption to run this test case is that test_jdbc_table
 * and meta-table are created already.
 */
public class ApplicationTest
{
  private static final String DB_DRIVER = "org.hsqldb.jdbcDriver";
  private static final String DB_URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";
  private static final String TABLE_NAME = "test_jdbc_table";

  @BeforeClass
  public static void setup()
  {
    try {
      Class.forName(DB_DRIVER).newInstance();

      Connection con = DriverManager.getConnection(DB_URL);
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
          + " (ACCOUNT_NO INTEGER, NAME VARCHAR(255),AMOUNT INTEGER)";
      stmt.executeUpdate(createTable);

    } catch (Throwable e) {
      DTThrowable.rethrow(e);
    }
  }

  public static void cleanTable()
  {
    try {
      Connection con = DriverManager.getConnection(DB_URL);
      Statement stmt = con.createStatement();
      String cleanTable = "delete from " + TABLE_NAME;
      stmt.executeUpdate(cleanTable);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public int getNumOfEventsInStore()
  {
    Connection con;
    try {
      con = DriverManager.getConnection(DB_URL);
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
  public void testCsvParserApp() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(new File("src/test/resources/test-FileToJdbcApp.xml").toURI().toURL());

      lma.prepareDAG(new FileToJdbcCsvParser(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync(); // test will terminate after results are available

      // wait for records to be added to table
      Thread.sleep(5000);

      Assert.assertEquals("Events in store", 10, getNumOfEventsInStore());
      cleanTable();

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  @Test
  public void testCustomParserApp() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(new File("src/test/resources/test-FileToJdbcApp.xml").toURI().toURL());

      lma.prepareDAG(new FileToJdbcCustomParser(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync(); // test will terminate after results are available

      // wait for records to be added to table
      Thread.sleep(5000);

      Assert.assertEquals("Events in store", 10, getNumOfEventsInStore());
      cleanTable();

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }
}

