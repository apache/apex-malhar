/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
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
  private static final String INMEM_DB_DRIVER = org.hsqldb.jdbcDriver.class.getName();
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

  private final static Exchanger<List<Object>> bulkValuesExchanger = new Exchanger<List<Object>>();

  public static class TestJDBCLookupCacheBackedOperator extends JDBCLookupCacheBackedOperator<String>
  {

    @Override
    public Integer getKeyFromTuple(String tuple)
    {
      return Integer.parseInt(tuple);
    }

    @Override
    public Map<Object, Object> loadInitialData()
    {
      return null;
    }

    @Override
    protected void preparePutStatement(PreparedStatement putStatement, Object key, Object value) throws SQLException
    {
      putStatement.setInt(1, (Integer) key);
      putStatement.setString(2, (String) value);

    }

    @Override
    protected void prepareGetStatement(PreparedStatement getStatement, Object key) throws SQLException
    {
      getStatement.setInt(1, (Integer) key);
    }

    @Override
    public Object processResultSet(ResultSet resultSet) throws SQLException
    {
      if (resultSet.next()) {
        return resultSet.getString(1);
      }
      return null;
    }

    @Override
    protected String fetchInsertQuery()
    {
      return "INSERT INTO " + TABLE_NAME + " (col1, col2) VALUES (?, ?)";
    }

    @Override
    protected String fetchGetQuery()
    {
      return "select col1, col2 from " + TABLE_NAME + " where col1 = ?";
    }

    @Override
    public List<Object> getAll(List<Object> keys)
    {
      List<Object> values = super.getAll(keys);
      try {
        bulkValuesExchanger.exchange(values);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return values;
    }

    @Override
    public void putAll(Map<Object, Object> m)
    {
    }

    @Override
    public void remove(Object key)
    {
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

    List<Object> bulk = bulkValuesExchanger.exchange(null, 30, TimeUnit.SECONDS);
    Assert.assertEquals("bulk values retrieval", 2, bulk.size());
  }

  @BeforeClass
  public static void setup() throws Exception
  {
    // This will load the JDBC driver, each DB has its own driver
    Class.forName(INMEM_DB_DRIVER).newInstance();

    Connection con = DriverManager.getConnection(INMEM_DB_URL);
    Statement stmt = con.createStatement();

    String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME
      + " (col1 INTEGER, col2 VARCHAR(20))";

    stmt.executeUpdate(createTable);
    stmt.executeUpdate("Delete from " + TABLE_NAME);

    // populate the database
    for (Map.Entry<Integer, String> entry : mapping.entrySet()) {
      String insert = "INSERT INTO " + TABLE_NAME
        + " (col1, col2) VALUES (" + entry.getKey() + ", '"
        + entry.getValue() + "')";
      stmt.executeUpdate(insert);
    }

    // Setup the operator
    lookupCacheBackedOperator.getStore().setDatabaseUrl(INMEM_DB_URL);
    lookupCacheBackedOperator.getStore().setDatabaseDriver(INMEM_DB_DRIVER);

    Calendar now = Calendar.getInstance();
    now.add(Calendar.SECOND, 5);

    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
    lookupCacheBackedOperator.getCacheManager().setRefreshTime(format.format(now.getTime()));

    lookupCacheBackedOperator.output.setSink(sink);

    Context.OperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(7);
    lookupCacheBackedOperator.setup(context);
  }

  @AfterClass
  public static void teardown() throws Exception
  {
  }

}
