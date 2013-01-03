/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.jdbc;

import com.malhartech.bufferserver.util.Codec;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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
  private static final int maxTuple = 20;
  private static final String driver = "com.mysql.jdbc.Driver";
  private static final String url = "jdbc:mysql://localhost/test?user=test&password=";
  private static final String db_name = "test";
  private static HashMap<String, ArrayList<String>> tableToColumns2 = new HashMap<String, ArrayList<String>>();
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
  public static void setupDB(JDBCOutputOperator oper, String[] mapping, boolean isHashMap)
  {
    int num = mapping.length;
    int colIdx = isHashMap ? 1 : 0;
    int typeIdx = isHashMap ? 2 : 1;
    HashMap<String, String> columnToType = new HashMap<String, String>();
    tableToColumns2.clear();

    String table;
    String column;

    for (int idx = 0; idx < num; ++idx) {
      String[] fields = mapping[idx].split(":");
      if (fields.length < 2 || fields.length > 3) {
        throw new RuntimeException("Incorrect column mapping for HashMap. Correct mapping should be Property:\"[Table.]Column:Type\"");
      }

      int colDelIdx = fields[colIdx].indexOf(".");
      if (colDelIdx != -1) { // table name is used
        table = fields[colIdx].substring(0, colDelIdx);
        column = fields[colIdx].substring(colDelIdx + 1);
      }
      else { // table name not used; so this must be single table
        table = oper.getTableName();
        if (table.isEmpty()) {
          throw new RuntimeException("Table name can not be empty");
        }
        column = fields[colIdx];
      }

      if (tableToColumns2.containsKey(table)) {
        tableToColumns2.get(table).add(column);
      }
      else {
        ArrayList<String> cols = new ArrayList<String>();
        cols.add(column);
        tableToColumns2.put(table, cols);
      }
      columnToType.put(column, fields[typeIdx]);
    }

    HashMap<String, String> tableToCreate = new HashMap<String, String>();
    for (Map.Entry<String, ArrayList<String>> entry: tableToColumns2.entrySet()) {
      String str = "";
      ArrayList<String> parts = entry.getValue();
      for (int i = 0; i < parts.size(); i++) {
        if (i == 0) {
          str += parts.get(i) + " " + columnToType.get(parts.get(i));
        }
        else {
          if (columnToType.get(parts.get(i)).equals("BAD_COLUMN_TYPE")) {
            str += ", " + parts.get(i) + " INTEGER";
          }
          else {
            str += ", " + parts.get(i) + " " + columnToType.get(parts.get(i));
          }
        }
      }

      str += ", winid BIGINT, operatorid VARCHAR(32), appid VARCHAR(32)";
      String createTable = "CREATE TABLE " + entry.getKey() + " (" + str + ")";
      tableToCreate.put(entry.getKey(), createTable);
      logger.debug(createTable);
    }

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

      for (Map.Entry<String, String> entry: tableToCreate.entrySet()) {
        stmt.execute("DROP TABLE IF EXISTS " + entry.getKey());
        stmt.executeUpdate(entry.getValue());
      }
      stmt.executeUpdate("CREATE TABLE IF NOT EXISTS maxwindowid (appid VARCHAR(10), winid BIGINT, operatorid VARCHAR(10))");
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException("Exception during JBDC connection", ex);
    }
    catch (SQLException ex) {
      throw new RuntimeException(String.format("Exception during setupDB"), ex);
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
    for (Map.Entry<String, ArrayList<String>> entry: tableToColumns2.entrySet()) {
      int num = entry.getValue().size();
      String query = "SELECT * FROM " + entry.getKey();
      try {
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
          String str = "";
          for (int i = 0; i < num; i++) {
            str += rs.getObject(i + 1).toString() + " ";
          }
          logger.debug(str);
        }
      }
      catch (SQLException ex) {
        throw new RuntimeException(String.format("Exception during reading from table %s", entry.getKey()), ex);
      }
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
      oper.setBatchSize(100);

      oper.setsWindowId("winid");
      oper.setsOperatorId("operatorid");
      oper.setsApplicationId("appid");
    }

    public void runTest(String opId, String[] mapping, boolean isHashMap)
    {
      oper.setColumnMapping(mapping);

      setupDB(oper, mapping, isHashMap);
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
      Assert.assertEquals("Number of emitted tuples", maxTuple, oper.getTupleCount());
      logger.debug(String.format("Number of emitted tuples: %d", oper.getTupleCount()));
    }
  }

  public void transactionOperatorSetting(JDBCTransactionOutputOperator oper)
  {
    oper.setMaxWindowTable("maxwindowid");
  }

  @Test
  public void JDBCHashMapOutputOperatorTest1() throws Exception
  {
    JDBCHashMapOutputOperator oper = new JDBCHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest("op1", helper.hashMapping1, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest2() throws Exception
  {
    JDBCHashMapOutputOperator oper = new JDBCHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    // You can set additional operator parameter here (see below) before calling runTest().
    oper.setBatchSize(6);
    oper.setTableName("Test_Tuple");
    tp.runTest("op2", helper.hashMapping2, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest3() throws Exception
  {
    JDBCHashMapOutputOperator oper = new JDBCHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest("op3", helper.hashMapping3, true);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest4() throws Exception
  {
    JDBCArrayListOutputOperator oper = new JDBCArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest("op4", helper.arrayMapping1, false);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest5() throws Exception
  {
    JDBCArrayListOutputOperator oper = new JDBCArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    oper.setTableName("Test_Tuple");
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
    transactionOperatorSetting(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest("op6", helper.arrayMapping3, false);

  }

  @Test
  public void JDBCHashMapOutputOperatorTest21() throws Exception
  {
    JDBCHashMapNonTransactionOutputOperator oper = new JDBCHashMapNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest("op21", helper.hashMapping1, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest22() throws Exception
  {
    JDBCHashMapNonTransactionOutputOperator oper = new JDBCHashMapNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    // You can set additional operator parameter here (see below) before calling runTest().
    oper.setBatchSize(6);
    oper.setTableName("Test_Tuple");
    tp.runTest("op22", helper.hashMapping2, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest23() throws Exception
  {
    JDBCHashMapNonTransactionOutputOperator oper = new JDBCHashMapNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest("op23", helper.hashMapping3, true);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest24() throws Exception
  {
    JDBCArrayListNonTransactionOutputOperator oper = new JDBCArrayListNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest("op24", helper.arrayMapping1, false);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest25() throws Exception
  {
    JDBCArrayListNonTransactionOutputOperator oper = new JDBCArrayListNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    oper.setTableName("Test_Tuple");
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
    oper.setTableName("Test_Tuple");
    tp.runTest("op26", helper.arrayMapping3, false);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest31() throws Exception
  {
    JDBCHashMapOutputOperator oper = new JDBCHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    // For mulit table mapping you don't need to set table name
    tp.runTest("op31", helper.hashMapping4, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest32() throws Exception
  {
    JDBCHashMapOutputOperator oper = new JDBCHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    tp.runTest("op32", helper.hashMapping5, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest33() throws Exception
  {
    JDBCHashMapNonTransactionOutputOperator oper = new JDBCHashMapNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op33", helper.hashMapping4, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest34() throws Exception
  {
    JDBCHashMapNonTransactionOutputOperator oper = new JDBCHashMapNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op34", helper.hashMapping5, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest41() throws Exception
  {
    JDBCArrayListOutputOperator oper = new JDBCArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    tp.runTest("op41", helper.arrayMapping4, false);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest42() throws Exception
  {
    JDBCArrayListOutputOperator oper = new JDBCArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    tp.runTest("op42", helper.arrayMapping5, false);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest43() throws Exception
  {
    JDBCArrayListNonTransactionOutputOperator oper = new JDBCArrayListNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op43", helper.arrayMapping4, false);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest44() throws Exception
  {
    JDBCArrayListNonTransactionOutputOperator oper = new JDBCArrayListNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op44", helper.arrayMapping5, false);
  }
}
