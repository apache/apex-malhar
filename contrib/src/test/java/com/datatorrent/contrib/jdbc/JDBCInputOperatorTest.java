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

import java.sql.*;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 *
 */
public class JDBCInputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCInputOperatorTest.class);
  private static final int maxTuple = 20;
  private int count = 0;
  private Connection con = null;
  private Statement stmt = null;
  private static final String driver = "com.mysql.jdbc.Driver";
  private static final String url = "jdbc:mysql://localhost/test?user=test&password=";
  private static final String db_name = "test";
  private String tableName = "Test_Tuple";

  /**
   * Test class for JDBCInputOperator
   */
  public class MyJDBCInputOperator extends JDBCInputOperator<String>
  {
    @Override
    public String queryToRetrieveData()
    {
      return "select * from Test_Tuple";
    }

    @Override
    public String getTuple(ResultSet result)
    {
      String tuple = "";
      try {
        for (int i = 0; i < 7; i++) {
          tuple += result.getObject(i + 1).toString() + " ";
        }
        count++;
        logger.debug(tuple);
      }
      catch (SQLException ex) {
        throw new RuntimeException("Error while creating tuple", ex);
      }

      return tuple;
    }
  }

  public void setupDB(String[] mapping)
  {
    try {
      // This will load the JDBC driver, each DB has its own driver
      Class.forName(driver).newInstance();

      con = DriverManager.getConnection(url);
      stmt = con.createStatement();

      String createDB = "CREATE DATABASE IF NOT EXISTS " + db_name;
      String useDB = "USE " + db_name;

      stmt.executeUpdate(createDB);
      stmt.executeQuery(useDB);

      String dropTable = "DROP TABLE IF EXISTS " + tableName;
      String createTable = "CREATE TABLE " + tableName + " (col1 INTEGER, col2 INTEGER, col5 INTEGER, col4 INTEGER, col7 INTEGER, col6 INTEGER, col3 INTEGER)";

      stmt.executeUpdate(dropTable);
      stmt.executeUpdate(createTable);

    }
    catch (InstantiationException ex) {
      throw new RuntimeException("Exception during setupDB", ex);
    }
    catch (IllegalAccessException ex) {
      throw new RuntimeException("Exception during setupDB", ex);
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException("Exception during setupDB", ex);
    }
    catch (SQLException ex) {
      throw new RuntimeException(String.format("Exception during setupDB"), ex);
    }
  }

  public void insertData(String[] mapping, JDBCOperatorTestHelper helper)
  {
    for (int i = 1; i <= maxTuple; ++i) {
      try {
        String insert = "INSERT INTO Test_Tuple (col1, col2, col5, col4, col7, col6, col3) VALUES (1, 2, 3, 4, 5, 6, 7)";
        stmt.executeUpdate(insert);
      }
      catch (SQLException ex) {
        throw new RuntimeException(String.format("Exception during insert data"), ex);
      }
    }
  }

  @Test
  public void JDBCInputOperatorTest() throws Exception
  {
    JDBCOperatorTestHelper helper = new JDBCOperatorTestHelper();
    helper.buildDataset();

    MyJDBCInputOperator oper = new MyJDBCInputOperator();
    oper.setDbUrl("jdbc:mysql://localhost/test?user=test&password=");
    oper.setDbDriver("com.mysql.jdbc.Driver");

    CollectorTestSink sink = new CollectorTestSink();
    oper.outputPort.setSink(sink);

    setupDB(helper.hashMapping1);

    oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(111));
    oper.beginWindow(1);
    insertData(helper.hashMapping1, helper);
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", maxTuple, count);
    logger.debug(String.format("Number of emitted tuples: %d", count));
  }
}
