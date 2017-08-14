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
package org.apache.apex.malhar.contrib.enrich;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.FieldInfo;

import com.datatorrent.netlet.util.DTThrowable;

public class JDBCLoaderTest
{
  static final Logger logger = LoggerFactory.getLogger(JDBCLoaderTest.class);

  public static class TestMeta extends TestWatcher
  {
    JDBCLoader dbloader;
    int[] id = {1, 2, 3, 4};
    String[] name = {"Paul", "Allen", "Teddy", "Mark"};
    int[] age = {32, 25, 23, 25};
    String[] address = {"California", "Texas", "Norway", "Rich-Mond"};
    double[] salary = {20000.00, 15000.00, 20000.00, 65000.00};

    @Override
    protected void starting(Description description)
    {
      try {
        dbloader = new JDBCLoader();
        dbloader.setDatabaseDriver("org.hsqldb.jdbcDriver");
        dbloader.setDatabaseUrl("jdbc:hsqldb:mem:test;sql.syntax_mys=true");
        dbloader.setTableName("COMPANY");

        dbloader.connect();
        createTable();
        insertRecordsInTable();
      } catch (Throwable e) {
        DTThrowable.rethrow(e);
      }
    }

    private void createTable()
    {
      try {
        Statement stmt = dbloader.getConnection().createStatement();

        String createTable = "CREATE TABLE " + dbloader.getTableName() + " " +
            "(ID INT PRIMARY KEY, " +
            "NAME CHAR(50), " +
            "AGE INT, " +
            "ADDRESS CHAR(50), " +
            "SALARY REAL)";
        logger.debug(createTable);
        stmt.executeUpdate(createTable);

        logger.debug("Table  created successfully...");
      } catch (Throwable e) {
        DTThrowable.rethrow(e);
      }
    }

    private void insertRecordsInTable()
    {
      try {
        Statement stmt = dbloader.getConnection().createStatement();
        String tbName = dbloader.getTableName();

        for (int i = 0; i < id.length; i++) {
          String sql = "INSERT INTO " + tbName + " (ID,NAME,AGE,ADDRESS,SALARY) " +
              "VALUES (" + id[i] + ", '" + name[i] + "', " + age[i] + ", '" + address[i] + "', " + salary[i] + " );";
          stmt.executeUpdate(sql);
        }
      } catch (Throwable e) {
        DTThrowable.rethrow(e);
      }

    }

    private void cleanTable()
    {
      String sql = "DROP TABLE " + dbloader.tableName;
      try {
        Statement stmt = dbloader.getConnection().createStatement();
        stmt.executeUpdate(sql);
        logger.debug("Table deleted successfully...");
      } catch (SQLException e) {
        DTThrowable.rethrow(e);
      }
    }

    @Override
    protected void finished(Description description)
    {
      cleanTable();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testMysqlDBLookup() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    ArrayList<FieldInfo> lookupKeys = new ArrayList<>();
    lookupKeys.add(new FieldInfo("ID", "ID", FieldInfo.SupportType.INTEGER));
    ArrayList<FieldInfo> includeKeys = new ArrayList<>();
    includeKeys.add(new FieldInfo("NAME", "NAME", FieldInfo.SupportType.STRING));
    includeKeys.add(new FieldInfo("AGE", "AGE", FieldInfo.SupportType.INTEGER));
    includeKeys.add(new FieldInfo("ADDRESS", "ADDRESS", FieldInfo.SupportType.STRING));

    testMeta.dbloader.setFieldInfo(lookupKeys, includeKeys);

    latch.await(1000, TimeUnit.MILLISECONDS);

    ArrayList<Object> keys = new ArrayList<>();
    keys.add(4);

    ArrayList<Object> columnInfo = (ArrayList<Object>)testMeta.dbloader.get(keys);

    Assert.assertEquals("NAME", "Mark", columnInfo.get(0).toString().trim());
    Assert.assertEquals("AGE", 25, columnInfo.get(1));
    Assert.assertEquals("ADDRESS", "Rich-Mond", columnInfo.get(2).toString().trim());
  }

  @Test
  public void testMysqlDBLookupIncludeAllKeys() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    ArrayList<FieldInfo> lookupKeys = new ArrayList<>();
    lookupKeys.add(new FieldInfo("ID", "ID", FieldInfo.SupportType.INTEGER));

    ArrayList<FieldInfo> includeKeys = new ArrayList<>();
    includeKeys.add(new FieldInfo("ID", "ID", FieldInfo.SupportType.INTEGER));
    includeKeys.add(new FieldInfo("NAME", "NAME", FieldInfo.SupportType.STRING));
    includeKeys.add(new FieldInfo("AGE", "AGE", FieldInfo.SupportType.INTEGER));
    includeKeys.add(new FieldInfo("ADDRESS", "ADDRESS", FieldInfo.SupportType.STRING));
    includeKeys.add(new FieldInfo("SALARY", "SALARY", FieldInfo.SupportType.DOUBLE));

    testMeta.dbloader.setFieldInfo(lookupKeys, includeKeys);

    latch.await(1000, TimeUnit.MILLISECONDS);

    ArrayList<Object> keys = new ArrayList<Object>();
    keys.add(4);

    ArrayList<Object> columnInfo = (ArrayList<Object>)testMeta.dbloader.get(keys);

    Assert.assertEquals("ID", 4, columnInfo.get(0));
    Assert.assertEquals("NAME", "Mark", columnInfo.get(1).toString().trim());
    Assert.assertEquals("AGE", 25, columnInfo.get(2));
    Assert.assertEquals("ADDRESS", "Rich-Mond", columnInfo.get(3).toString().trim());
    Assert.assertEquals("SALARY", 65000.0, columnInfo.get(4));
  }

  @Test
  public void testMysqlDBLookupIncludeAllKeysEmptyQuery() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    ArrayList<FieldInfo> lookupKeys = new ArrayList<>();
    lookupKeys.add(new FieldInfo("ID", "ID", FieldInfo.SupportType.INTEGER));

    ArrayList<FieldInfo> includeKeys = new ArrayList<>();
    includeKeys.add(new FieldInfo("ID", "ID", FieldInfo.SupportType.INTEGER));
    includeKeys.add(new FieldInfo("NAME", "NAME", FieldInfo.SupportType.STRING));
    includeKeys.add(new FieldInfo("AGE", "AGE", FieldInfo.SupportType.INTEGER));
    includeKeys.add(new FieldInfo("ADDRESS", "ADDRESS", FieldInfo.SupportType.STRING));
    includeKeys.add(new FieldInfo("SALARY", "SALARY", FieldInfo.SupportType.DOUBLE));

    testMeta.dbloader.setQueryStmt("");
    testMeta.dbloader.setFieldInfo(lookupKeys, includeKeys);

    latch.await(1000, TimeUnit.MILLISECONDS);

    ArrayList<Object> keys = new ArrayList<Object>();
    keys.add(4);

    ArrayList<Object> columnInfo = (ArrayList<Object>)testMeta.dbloader.get(keys);

    Assert.assertEquals("ID", 4, columnInfo.get(0));
    Assert.assertEquals("NAME", "Mark", columnInfo.get(1).toString().trim());
    Assert.assertEquals("AGE", 25, columnInfo.get(2));
    Assert.assertEquals("ADDRESS", "Rich-Mond", columnInfo.get(3).toString().trim());
    Assert.assertEquals("SALARY", 65000.0, columnInfo.get(4));
  }

  @Test
  public void testMysqlDBQuery() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    testMeta.dbloader
        .setQueryStmt("Select id, name from " + testMeta.dbloader.getTableName() + " where AGE = ? and ADDRESS = ?");

    latch.await(1000, TimeUnit.MILLISECONDS);

    ArrayList<FieldInfo> includeKeys = new ArrayList<>();
    includeKeys.add(new FieldInfo("ID", "ID", FieldInfo.SupportType.INTEGER));
    includeKeys.add(new FieldInfo("NAME", "NAME", FieldInfo.SupportType.STRING));

    testMeta.dbloader.setFieldInfo(null, includeKeys);

    ArrayList<Object> keys = new ArrayList<Object>();
    keys.add(25);
    keys.add("Texas");

    ArrayList<Object> columnInfo = (ArrayList<Object>)testMeta.dbloader.get(keys);

    Assert.assertEquals("ID", 2, columnInfo.get(0));
    Assert.assertEquals("NAME", "Allen", columnInfo.get(1).toString().trim());
  }
}
