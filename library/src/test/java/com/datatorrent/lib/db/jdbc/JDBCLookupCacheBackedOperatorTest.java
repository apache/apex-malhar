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
package com.datatorrent.lib.db.jdbc;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Test for {@link JDBCLookupCacheBackedOperator}
 */
public class JDBCLookupCacheBackedOperatorTest
{
  private static final String INMEM_DB_URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";
  private static final String INMEM_DB_DRIVER = "org.hsqldb.jdbcDriver";
  protected static final String TABLE_NAME = "Test_Lookup_Cache";

  protected static TestJDBCLookupCacheBackedOperator lookupCacheBackedOperator = new TestJDBCLookupCacheBackedOperator();
  protected static CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
  protected static final Map<Integer, String> mapping = Maps.newHashMap();

  static {
    mapping.put(1, "one");
    mapping.put(2, "two");
    mapping.put(3, "three");
    mapping.put(4, "four");
    mapping.put(5, "five");
  }

  protected static transient final Logger logger = LoggerFactory.getLogger(JDBCLookupCacheBackedOperatorTest.class);

  private final static Exchanger<Map<Object, Object>> bulkValuesExchanger = new Exchanger<Map<Object, Object>>();

  public static class TestJDBCLookupCacheBackedOperator extends JDBCLookupCacheBackedOperator<String>
  {

    @Override
    public Integer getKeyFromTuple(String tuple)
    {
      return Integer.parseInt(tuple);
    }

    @Override
    public Object getValueFor(Object key)
    {
      String query = "select col2 from " + TABLE_NAME + " where col1 = " + key;
      Statement stmt;
      try {
        stmt = store.connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(query);
        resultSet.next();
        Object value = resultSet.getString(1);
        stmt.close();
        resultSet.close();
        return value;
      }
      catch (SQLException e) {
        throw new RuntimeException("while fetching key", e);
      }
    }

    @Override
    public Map<Object, Object> bulkGet(Set<Object> keys)
    {
      StringBuilder builder = new StringBuilder("(");
      for (Object k : keys) {
        builder.append(k);
        builder.append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
      builder.append(")");
      String query = "select col1, col2 from " + TABLE_NAME + " where col1 in " + builder.toString();

      try {
        Statement statement = store.connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        Map<Object, Object> values = Maps.newHashMap();
        while (resultSet.next()) {
          values.put(resultSet.getInt(1), resultSet.getString(2));
        }
        bulkValuesExchanger.exchange(values);
        return values;
      }
      catch (SQLException e) {
        throw new RuntimeException("while fetching multiple keys", e);
      }
      catch (InterruptedException e) {
        throw new RuntimeException("interrupted while multiple keys", e);
      }
    }

    @Override
    public Map<Object, Object> fetchStartupData()
    {
      return null;
    }

  }

  @Test
  public void test() throws Exception
  {
    lookupCacheBackedOperator.beginWindow(0);
    lookupCacheBackedOperator.input.process("1");
    lookupCacheBackedOperator.input.process("2");
    lookupCacheBackedOperator.endWindow();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", 2, sink.collectedTuples.size());

    Map<Object, Object> bulk = bulkValuesExchanger.exchange(null, 30, TimeUnit.SECONDS);
    Assert.assertEquals("bulk values retrieval", 2, bulk.size());
  }

  @BeforeClass
  public static void setup() throws Exception
  {
    // This will load the JDBC driver, each DB has its own driver
    Class.forName(INMEM_DB_DRIVER).newInstance();

    Connection con = DriverManager.getConnection(INMEM_DB_URL);
    Statement stmt = con.createStatement();

    String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (col1 INTEGER, col2 VARCHAR(20))";

    stmt.executeUpdate(createTable);
    stmt.executeUpdate("Delete from " + TABLE_NAME);

    //populate the database
    for (Map.Entry<Integer, String> entry : mapping.entrySet()) {
      String insert = "INSERT INTO " + TABLE_NAME + " (col1, col2) VALUES (" + entry.getKey() + ", '" + entry.getValue() + "')";
      stmt.executeUpdate(insert);
    }

    //Setup the operator
    lookupCacheBackedOperator.store.setDbUrl(INMEM_DB_URL);
    lookupCacheBackedOperator.store.setDbDriver(INMEM_DB_DRIVER);

    Calendar now = Calendar.getInstance();
    now.add(Calendar.SECOND, 5);

    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
    lookupCacheBackedOperator.setCacheRefreshTime(format.format(now.getTime()));

    lookupCacheBackedOperator.output.setSink(sink);

    Context.OperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(7);
    lookupCacheBackedOperator.setup(context);
  }

  @AfterClass
  public static void teardown() throws Exception
  {
  }

}
