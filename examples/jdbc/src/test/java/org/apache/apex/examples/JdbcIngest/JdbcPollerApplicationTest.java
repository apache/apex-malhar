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
package org.apache.apex.examples.JdbcIngest;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import javax.validation.ConstraintViolationException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

public class JdbcPollerApplicationTest
{
  private static final String DB_DRIVER = "org.hsqldb.jdbcDriver";
  private static final String URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";
  private static final String TABLE_NAME = "test_event_table";
  private static final String OUTPUT_DIR_NAME = "./target/tmp/test/output";

  @BeforeClass
  public static void setup()
  {
    try {
      cleanup();
      dropTable();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    try {
      Class.forName(DB_DRIVER).newInstance();

      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();

      String createTable = "CREATE TABLE " + TABLE_NAME
          + " (ACCOUNT_NO INTEGER, NAME VARCHAR(255),AMOUNT INTEGER)";
      stmt.executeUpdate(createTable);
      insertEventsInTable(10, 0);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void cleanup()
  {
    try {
      FileUtils.deleteDirectory(new File(OUTPUT_DIR_NAME));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void dropTable()
  {
    try {
      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();
      String cleanTable = "DROP TABLE IF EXISTS " + TABLE_NAME;
      stmt.executeUpdate(cleanTable);
    } catch (SQLException e) {
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

  @Test
  public void testApplication() throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.set("dt.application.PollJdbcToHDFSApp.operator.JdbcPoller.prop.store.databaseUrl", URL);
      conf.set("dt.application.PollJdbcToHDFSApp.operator.JdbcPoller.prop.store.databaseDriver", DB_DRIVER);
      conf.setInt("dt.application.PollJdbcToHDFSApp.operator.JdbcPoller.prop.partitionCount", 2);
      conf.set("dt.application.PollJdbcToHDFSApp.operator.JdbcPoller.prop.key", "ACCOUNT_NO");
      conf.set("dt.application.PollJdbcToHDFSApp.operator.JdbcPoller.prop.columnsExpression", "ACCOUNT_NO,NAME,AMOUNT");
      conf.set("dt.application.PollJdbcToHDFSApp.operator.JdbcPoller.prop.tableName", TABLE_NAME);
      conf.set("dt.application.PollJdbcToHDFSApp.operator.JdbcPoller.port.outputPort.attr.TUPLE_CLASS",
          "org.apache.apex.examples.JdbcIngest.PojoEvent");
      conf.set("dt.application.PollJdbcToHDFSApp.operator.Writer.filePath", OUTPUT_DIR_NAME);

      lma.prepareDAG(new JdbcPollerApplication(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      // wait for output files to roll
      Thread.sleep(45000);

      String[] extensions = {"dat.0","tmp"};
      Collection<File> list = FileUtils.listFiles(new File(OUTPUT_DIR_NAME), extensions, false);
      int recordsCount = 0;
      for (File file : list) {
        recordsCount += FileUtils.readLines(file).size();
      }
      Assert.assertEquals("Records in file", 10, recordsCount);

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }
}
