/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.jdbc;

import com.datatorrent.lib.database.CacheHandler;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.sql.*;
import java.util.Map;

/**
 * Test for {@link JDBCLookupCacheBackedOperator}
 */
public class JDBCLookupCacheBackedOperatorTest
{

  private static final String URL = "jdbc:mysql://localhost/test?user=test&password=";
  private static final String DB_NAME = "test";
  private static final String TABLE_NAME = "Test_Lookup_Cache";
  private static final String DB_DRIVER = "com.mysql.jdbc.Driver";

  private static final Map<Integer, String> mapping = Maps.newHashMap();

  static {
    mapping.put(1, "one");
    mapping.put(2, "two");
    mapping.put(3, "three");
    mapping.put(4, "four");
    mapping.put(5, "five");
  }

  public class TestJDBCLookupCacheBackedOperator extends JDBCLookupCacheBackedOperator<String, Integer>
  {

    @Override
    public Integer getKeyFromTuple(String tuple)
    {
      return Integer.parseInt(tuple);
    }

    @Override
    public String getQueryToFetchTheKeyFromDb(Integer key)
    {
      return "select col2 from " + DB_NAME + "." + TABLE_NAME + " where col1 = " + key;
    }

    @Nullable
    @Override
    public Object getValueFromResultSet(ResultSet resultSet)
    {
      try {
        resultSet.next();
        return resultSet.getString(1);
      } catch (SQLException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void test() throws Exception
  {
    JDBCOperatorTestHelper helper = new JDBCOperatorTestHelper();
    helper.buildDataset();

    TestJDBCLookupCacheBackedOperator oper = new TestJDBCLookupCacheBackedOperator();
    oper.setDbUrl("jdbc:mysql://localhost/test?user=test&password=");
    oper.setDbDriver("com.mysql.jdbc.Driver");

    CollectorTestSink sink = new CollectorTestSink();
    oper.outputPort.setSink(sink);

    setupDB();

    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(7));
    oper.beginWindow(0);
    oper.inputData.process("1");
    oper.inputData.process("2");
    oper.endWindow();
    oper.teardown();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", 2, sink.collectedTuples.size());
  }

  private void setupDB()
  {
    try {
      // This will load the JDBC driver, each DB has its own driver
      Class.forName(DB_DRIVER).newInstance();

      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();

      String createDB = "CREATE DATABASE IF NOT EXISTS " + DB_NAME;
      String useDB = "USE " + DB_NAME;

      stmt.executeUpdate(createDB);
      stmt.executeQuery(useDB);

      String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (col1 INTEGER, col2 VARCHAR(20))";

      stmt.executeUpdate(createTable);

      //populate the database
      for (Map.Entry<Integer, String> entry : mapping.entrySet()) {
        String insert = "INSERT INTO " + TABLE_NAME + " (col1, col2) VALUES (" + entry.getKey() + ", '" + entry.getValue() + "')";
        stmt.executeUpdate(insert);
      }

    } catch (InstantiationException ex) {
      throw new RuntimeException("Exception during setupDB", ex);
    } catch (IllegalAccessException ex) {
      throw new RuntimeException("Exception during setupDB", ex);
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException("Exception during setupDB", ex);
    } catch (SQLException ex) {
      throw new RuntimeException(String.format("Exception during setupDB"), ex);
    }
  }
}
