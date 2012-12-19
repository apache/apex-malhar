/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.bufferserver.util.Codec;
import java.sql.*;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCOutputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCOutputOperatorTest.class);
  private static int tupleCount = 0;
  private static final int maxTuple = 20;
  private static final String driver = "com.mysql.jdbc.Driver";
  private static final String url = "jdbc:mysql://localhost/test?user=test&password=";
  private static final String db_name = "test";
  private static Connection con = null;
  private static Statement stmt = null;
  JDBCOperatorTestHelper helper = new JDBCOperatorTestHelper();

  public JDBCOutputOperatorTest()
  {
    helper.buildDataset();
  }

  /*
   * Todo:
   * - Handle null name in column hashMapping2
   * - in arraylist if tuple has less or more columns
   * - embedded sql
   * - multi table support
   * -
   *
   * Done
   * - AssertFalse for negative test
   * - dbuser/pw cleanup
   * - refactor unit tests
   * - fix unit test if no database exist
   */

  /*
   * Create database connection.
   * Create database if not exist.
   * Create two tables - one for tuple, one for maxwindowid.
   */
  public static void setupDB(String tableName, String[] mapping, boolean isHashMap)
  {
    int num = mapping.length;
    int colIdx = isHashMap ? 1 : 0;
    int typeIdx = isHashMap ? 2 : 1;

    String str = "";
    for (int i = 0; i < num; i++) {
      String[] parts = mapping[i].split(":");
      if (i == 0) {
        str += parts[colIdx] + " " + parts[typeIdx];
      }
      else {
        if (parts[typeIdx].equals("BAD_COLUMN_TYPE")) {
          str += ", " + parts[colIdx] + " INTEGER";
        }
        else {
          str += ", " + parts[colIdx] + " " + parts[typeIdx];
        }
      }
    }

    str += ", winid BIGINT";
    String createTable = "CREATE TABLE " + tableName + " (" + str + ")";

    try {
      // This will load the JDBC driver, each DB has its own driver
      Class.forName(driver).newInstance();
      //String temp = "jdbc:derby:test;create=true";

      con = DriverManager.getConnection(url);
      stmt = con.createStatement();

      String createDB = "CREATE DATABASE IF NOT EXISTS " + db_name;
      String useDB = "USE " + db_name;

      stmt.executeUpdate(createDB);
      stmt.executeQuery(useDB);

      stmt.execute("DROP TABLE IF EXISTS " + tableName);
      stmt.executeUpdate(createTable);

      stmt.executeUpdate("CREATE TABLE IF NOT EXISTS maxwindowid (appid VARCHAR(10), winid BIGINT, operatorid VARCHAR(10))");
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException("Exception during JBDC connection", ex);
    }
    catch (SQLException ex) {
      throw new RuntimeException(String.format("Exception during creating table with query: %s", createTable), ex);
    }
    catch (Exception ex) {
      throw new RuntimeException("Exception during JBDC connection", ex);
    }

    logger.debug("JDBC Table creation Success");
  }

  /*
   * Read tuple from database after running the operator.
   */
  public static void readDB(String tableName, String[] mapping, boolean isHashMap)
  {
    int num = mapping.length;
    String query = "SELECT * FROM " + tableName;
    try {
      ResultSet rs = stmt.executeQuery(query);
      while (rs.next()) {
        String str = "";
        for (int i = 0; i < num; i++) {
          str += rs.getObject(i + 1).toString() + " ";
        }
        logger.debug(str);
        tupleCount++;
      }
    }
    catch (SQLException ex) {
      throw new RuntimeException("Exception during reading from table", ex);
    }
  }

  /*
   * Close database resources.
   */
  public void cleanupDB()
  {
    try {
      stmt.close();
      con.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException("Exception while closing database resource", ex);
    }
  }

  /*
   * Template for running test.
   */
  public class JDBCOutputOperatorTestTemplate
  {
    JDBCOutputOperator oper = null;

    JDBCOutputOperatorTestTemplate(JDBCOutputOperator op)
    {
      oper = op;
      oper.setDbUrl("jdbc:mysql://localhost/test?user=test&password=");
      oper.setDbDriver("com.mysql.jdbc.Driver");
      oper.setTableName("Test_Tuple");
      oper.setBatchSize(100);
    }

    public void runTest(String opId, String[] mapping, boolean isHashMap)
    {
      tupleCount = 0; // reset
      oper.setColumnMapping(mapping);

      setupDB(oper.getTableName(), mapping, isHashMap);
      oper.setup(new com.malhartech.engine.OperatorContext(opId, null, null));
      oper.beginWindow(oper.lastWindowId + 1);
      logger.debug("beginwindow {}", Codec.getStringWindowId(oper.lastWindowId + 1));

      for (int i = 0; i < maxTuple; ++i) {
        oper.inputPort.process(isHashMap ? helper.hashMapData(mapping, i) : helper.arrayListData(mapping, i));
      }
      oper.endWindow();
      readDB(oper.getTableName(), mapping, isHashMap);

      oper.teardown();
      cleanupDB();

      // Check values send vs received
      Assert.assertEquals("Number of emitted tuples", maxTuple, tupleCount);
      logger.debug(String.format("Number of emitted tuples: %d", tupleCount));
    }
  }

  @Test
  public void JDBCHashMapOutputOperatorTest() throws Exception
  {
    JDBCHashMapOutputOperator oper = new JDBCHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op1", helper.hashMapping1, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest2() throws Exception
  {
    JDBCHashMapOutputOperator oper = new JDBCHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    // You can set additional operator parameter here (see below) before calling runTest().
    oper.setBatchSize(6);
    tp.runTest("op2", helper.hashMapping2, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest3() throws Exception
  {
    JDBCHashMapOutputOperator oper = new JDBCHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op3", helper.hashMapping3, true);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest4() throws Exception
  {
    JDBCArrayListOutputOperator oper = new JDBCArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op4", helper.arrayMapping1, false);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest5() throws Exception
  {
    JDBCArrayListOutputOperator oper = new JDBCArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    try {
      tp.runTest("op5", helper.arrayMapping2, false);
      Assert.assertFalse("This test failed if it ever comes to this line", true);
    }
    catch (Exception ex) {
      logger.debug("Expected exception for bad column type: ", ex.getLocalizedMessage());
    }
  }

  @Test
  public void JDBCArrayListOutputOperatorTest6() throws Exception
  {
    JDBCArrayListOutputOperator oper = new JDBCArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op6", helper.arrayMapping3, false);

  }

  @Test
  public void JDBCHashMapOutputOperatorTest21() throws Exception
  {
    JDBCHashMapNonTransactionOutputOperator oper = new JDBCHashMapNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op21", helper.hashMapping1, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest22() throws Exception
  {
    JDBCHashMapNonTransactionOutputOperator oper = new JDBCHashMapNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    // You can set additional operator parameter here (see below) before calling runTest().
    oper.setBatchSize(6);
    tp.runTest("op22", helper.hashMapping2, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest23() throws Exception
  {
    JDBCHashMapNonTransactionOutputOperator oper = new JDBCHashMapNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op23", helper.hashMapping3, true);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest24() throws Exception
  {
    JDBCArrayListNonTransactionOutputOperator oper = new JDBCArrayListNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op24", helper.arrayMapping1, false);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest25() throws Exception
  {
    JDBCArrayListNonTransactionOutputOperator oper = new JDBCArrayListNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    try {
      tp.runTest("op25", helper.arrayMapping2, false);
      Assert.assertFalse("This test failed if it ever comes to this line", true);
    }
    catch (Exception ex) {
      logger.debug("Expected exception for bad column type: ", ex.getLocalizedMessage());
    }
  }

  @Test
  public void JDBCArrayListOutputOperatorTest26() throws Exception
  {
    JDBCArrayListNonTransactionOutputOperator oper = new JDBCArrayListNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op26", helper.arrayMapping3, false);
  }
}
