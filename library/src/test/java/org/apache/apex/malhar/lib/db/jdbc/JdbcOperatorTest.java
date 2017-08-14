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
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.BeforeClass;

import com.datatorrent.netlet.util.DTThrowable;

public class JdbcOperatorTest
{
  public static final String DB_DRIVER = "org.hsqldb.jdbcDriver";
  public static final String URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";

  protected static final String TABLE_NAME = "test_event_table";
  protected static final String TABLE_POJO_NAME = "test_pojo_event_table";
  protected static final String TABLE_POJO_NAME_ID_DIFF = "test_pojo_event_table_id_diff";
  protected static final String TABLE_POJO_NAME_NAME_DIFF = "test_pojo_event_table_name_diff";
  protected static String APP_ID = "JdbcOperatorTest";
  protected static int OPERATOR_ID = 0;

  public static class TestEvent
  {
    int id;

    TestEvent(int id)
    {
      this.id = id;
    }
  }

  public static class TestPOJOEvent
  {
    private int id;
    private String name;
    private Date startDate;
    private Time startTime;
    private Timestamp startTimestamp;
    private double score;

    public TestPOJOEvent()
    {
    }

    public TestPOJOEvent(int id, String name)
    {
      this.id = id;
      this.name = name;
    }

    public int getId()
    {
      return id;
    }

    public void setId(int id)
    {
      this.id = id;
    }

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public Date getStartDate()
    {
      return startDate;
    }

    public void setStartDate(Date startDate)
    {
      this.startDate = startDate;
    }

    public Time getStartTime()
    {
      return startTime;
    }

    public void setStartTime(Time startTime)
    {
      this.startTime = startTime;
    }

    public Timestamp getStartTimestamp()
    {
      return startTimestamp;
    }

    public void setStartTimestamp(Timestamp startTimestamp)
    {
      this.startTimestamp = startTimestamp;
    }

    public double getScore()
    {
      return score;
    }

    public void setScore(double score)
    {
      this.score = score;
    }

    @Override
    public String toString()
    {
      return "TestPOJOEvent [id=" + id + ", name=" + name + ", startDate=" + startDate + ", startTime=" + startTime
          + ", startTimestamp=" + startTimestamp + ", score=" + score + "]";
    }


  }

  @BeforeClass
  public static void setup()
  {
    try {
      Class.forName(DB_DRIVER).newInstance();

      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();

      String createMetaTable = "CREATE TABLE IF NOT EXISTS " + JdbcTransactionalStore.DEFAULT_META_TABLE + " ( " + JdbcTransactionalStore.DEFAULT_APP_ID_COL + " VARCHAR(100) NOT NULL, " + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT NOT NULL, " + JdbcTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT NOT NULL, " + "UNIQUE (" + JdbcTransactionalStore.DEFAULT_APP_ID_COL + ", " + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + ", " + JdbcTransactionalStore.DEFAULT_WINDOW_COL + ") " + ")";
      stmt.executeUpdate(createMetaTable);

      String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (ID INTEGER)";
      stmt.executeUpdate(createTable);
      String createPOJOTable = "CREATE TABLE IF NOT EXISTS " + TABLE_POJO_NAME
          + "(id INTEGER not NULL,name VARCHAR(255),startDate DATE,startTime TIME,startTimestamp TIMESTAMP, score DOUBLE, PRIMARY KEY ( id ))";

      stmt.executeUpdate(createPOJOTable);
      String createPOJOTableIdDiff = "CREATE TABLE IF NOT EXISTS " + TABLE_POJO_NAME_ID_DIFF + "(id1 INTEGER not NULL,name VARCHAR(255), PRIMARY KEY ( id1 ))";
      stmt.executeUpdate(createPOJOTableIdDiff);
      String createPOJOTableNameDiff = "CREATE TABLE IF NOT EXISTS " + TABLE_POJO_NAME_NAME_DIFF + "(id INTEGER not NULL,name1 VARCHAR(255), PRIMARY KEY ( id ))";
      stmt.executeUpdate(createPOJOTableNameDiff);
    } catch (Throwable e) {
      DTThrowable.rethrow(e);
    }
  }

  public static void cleanTable()
  {
    try {
      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();

      String cleanTable = "delete from " + TABLE_NAME;
      stmt.executeUpdate(cleanTable);

      cleanTable = "delete from " + TABLE_POJO_NAME;
      stmt.executeUpdate(cleanTable);

      cleanTable = "delete from " + JdbcTransactionalStore.DEFAULT_META_TABLE;
      stmt.executeUpdate(cleanTable);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static void insertEventsInTable(int numEvents)
  {
    try {
      Connection con = DriverManager.getConnection(URL);
      String insert = "insert into " + TABLE_NAME + " values (?)";
      PreparedStatement stmt = con.prepareStatement(insert);

      for (int i = 0; i < numEvents; i++) {
        stmt.setInt(1, i);
        stmt.executeUpdate();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected static void insertEvents(int numEvents, boolean cleanExistingRows, int startRowId)
  {
    try (Connection con = DriverManager.getConnection(URL); Statement stmt = con.createStatement()) {
      if (cleanExistingRows) {
        String cleanTable = "delete from " + TABLE_POJO_NAME;
        stmt.executeUpdate(cleanTable);
      }

      String insert = "insert into " + TABLE_POJO_NAME + " values (?,?,?,?,?,?)";
      PreparedStatement pStmt = con.prepareStatement(insert);
      con.prepareStatement(insert);

      for (int i = 0; i < numEvents; i++) {
        pStmt.setInt(1, startRowId + i);
        pStmt.setString(2, "name" + i);
        pStmt.setDate(3, new Date(2016, 1, 1));
        pStmt.setTime(4, new Time(2016, 1, 1));
        pStmt.setTimestamp(5, new Timestamp(2016, 1, 1, 0, 0, 0, 0));
        pStmt.setDouble(6, new Double(55.4));
        pStmt.executeUpdate();
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
