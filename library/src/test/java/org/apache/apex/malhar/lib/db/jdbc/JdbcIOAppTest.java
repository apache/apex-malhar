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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 * App test for {@link JdbcPOJOInputOperator and JdbcPOJOOutputOperator}
 */
public class JdbcIOAppTest
{
  public static final String DB_DRIVER = "org.hsqldb.jdbcDriver";
  public static final String URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";

  private static final String TABLE_NAME = "test_app_event_table";
  private static final String OUTPUT_TABLE_NAME = "test_app_output_event_table";

  @BeforeClass
  public static void setup()
  {
    try {

      Class.forName(DB_DRIVER).newInstance();

      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();

      String createMetaTable = "CREATE TABLE IF NOT EXISTS " + JdbcTransactionalStore.DEFAULT_META_TABLE + " ( "
          + JdbcTransactionalStore.DEFAULT_APP_ID_COL + " VARCHAR(100) NOT NULL, "
          + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT NOT NULL, "
          + JdbcTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT NOT NULL, " + "UNIQUE ("
          + JdbcTransactionalStore.DEFAULT_APP_ID_COL + ", " + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + ", "
          + JdbcTransactionalStore.DEFAULT_WINDOW_COL + ") " + ")";
      stmt.executeUpdate(createMetaTable);

      Class.forName(DB_DRIVER).newInstance();

      String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME
          + " (ACCOUNT_NO INTEGER, NAME VARCHAR(255),AMOUNT INTEGER)";
      stmt.executeUpdate(createTable);
      insertEventsInTable(10, 0);

      String createOutputTable = "CREATE TABLE IF NOT EXISTS " + OUTPUT_TABLE_NAME
          + " (ACCOUNT_NO INTEGER, NAME VARCHAR(255),AMOUNT INTEGER)";
      stmt.executeUpdate(createOutputTable);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void insertEventsInTable(int numEvents, int offset)
  {
    try {
      Connection con = DriverManager.getConnection(URL);
      String insert = "insert into " + TABLE_NAME + " values (?,?,?)";
      PreparedStatement stmt = con.prepareStatement(insert);
      for (int i = 0; i < numEvents; i++, offset++) {
        stmt.setInt(1, offset);
        stmt.setString(2, "Account_Holder-" + offset);
        stmt.setInt(3, (offset * 1000));
        stmt.executeUpdate();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public int getNumOfEventsInStore()
  {
    Connection con;
    try {
      con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();

      String countQuery = "SELECT count(*) from " + OUTPUT_TABLE_NAME;
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
      lma.prepareDAG(new JdbcIOApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();
      // wait for records to be added to table
      Thread.sleep(3000);
      lc.shutdown();
      Assert.assertEquals("Events in store", 10, getNumOfEventsInStore());
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  public static class PojoEvent
  {
    @Override
    public String toString()
    {
      return "PojoEvent [accountNumber=" + accountNumber + ", name=" + name + ", amount=" + amount + "]";
    }

    private int accountNumber;
    private String name;
    private int amount;

    public int getAccountNumber()
    {
      return accountNumber;
    }

    public void setAccountNumber(int accountNumber)
    {
      this.accountNumber = accountNumber;
    }

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public int getAmount()
    {
      return amount;
    }

    public void setAmount(int amount)
    {
      this.amount = amount;
    }
  }
}
