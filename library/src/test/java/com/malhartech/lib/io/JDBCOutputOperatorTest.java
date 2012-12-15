/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.bufferserver.util.Codec;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
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
  private static int dataset = 1;
  JDBCOperatorTestHelper helper = new JDBCOperatorTestHelper();

  public JDBCOutputOperatorTest()
  {
    helper.buildDataset();
  }

  /*
   * Todo:
   * - Handle null name in column hashMapping2
   * - embedded sql
   * - multi table support
   * - refactor unit tests
   * - fix unit test if no database exist
   * - in arraylist if tuple has less or more columns
   */
  public static void createDatabase(String dbName, Connection con)
  {
    Statement stmt = null;
    try {
      stmt = con.createStatement();

      String createDB = "CREATE DATABASE IF NOT EXISTS " + dbName;
      String useDB = "USE " + dbName;

      stmt.executeUpdate(createDB);
      stmt.executeQuery(useDB);
    }
    catch (SQLException ex) {
      throw new RuntimeException("Exception during creating database", ex);
    }
    finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex) {
        throw new RuntimeException("Exception while closing database resource", ex);
      }
    }

    logger.debug("JDBC DB creation Success");
  }

  public static void createTable(String tableName, Connection con, ArrayList<String> columns, HashMap<String, String> colTypes) throws Exception
  {
    int num = columns.size();
    String cols = columns.get(0) + " " + colTypes.get(columns.get(0));
    for (int i = 1; i < num; i++) {
      // Handle negative test case
      if ("BAD_COLUMN_TYPE".equals(colTypes.get(columns.get(i)))) {
        cols += ", " + columns.get(i) + " INTEGER";
      }
      else {
        cols += ", " + columns.get(i) + " " + colTypes.get(columns.get(i));
      }
    }

    cols += ", winid BIGINT";
    String str = "CREATE TABLE " + tableName + " (" + cols + ")";

    Statement stmt = null;
    try {
      stmt = con.createStatement();
      stmt.execute("DROP TABLE IF EXISTS " + tableName);
      stmt.executeUpdate(str);
    }
    catch (SQLException ex) {
      throw new RuntimeException("Exception during creating table", ex);
    }
    finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex) {
        throw new RuntimeException("Exception while closing database resource", ex);
      }
    }
    logger.debug("JDBC Table creation Success");
  }

  public static void readTable(String tableName, Connection con)
  {
    String query = "SELECT * FROM " + tableName;
    Statement stmt = null;
    try {
      stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery(query);

      while (rs.next()) {
        if (dataset == 1) {
          logger.debug(String.format("%d, %d, %d, %d, %d, %d, %d",
                                     rs.getInt("col1"), rs.getInt("col2"), rs.getInt("col3"),
                                     rs.getInt("col4"), rs.getInt("col5"), rs.getInt("col6"),
                                     rs.getInt("col7")));
        }
        else if (dataset == 2) {
          logger.debug(String.format("%d, %s, %d, %s, %d, %s, %d",
                                     rs.getInt("col1"), rs.getString("col2"),
                                     rs.getInt("col3"), rs.getString("col4"),
                                     rs.getInt("col5"), rs.getString("col6"),
                                     rs.getInt("col7")));
        }
        else if (dataset == 3) {
          logger.debug(String.format("%d, %d, %s, %s, %s, %s, %f",
                                     rs.getInt("col1"), rs.getInt("col2"),
                                     rs.getDate("col3"), rs.getDate("col4"),
                                     rs.getString("col5"), rs.getString("col6"),
                                     rs.getDouble("col7")));
        }
        tupleCount++;
      }
    }
    catch (SQLException ex) {
      throw new RuntimeException("Exception during reading from table", ex);
    }
    finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex) {
        throw new RuntimeException("Exception while closing database resource", ex);
      }
    }
  }

  public class JDBCOutputOperatorTestTemplate
  {
    JDBCOutputOperator oper = null;

    JDBCOutputOperatorTestTemplate(JDBCOutputOperator op)
    {
      oper = op;
      oper.setDbUrl("jdbc:mysql://localhost/");
      oper.setDbName("test");
      oper.setDbUser("test");
      oper.setDbPassword("");
      oper.setDbDriver("com.mysql.jdbc.Driver");
      oper.setTableName("Test_Tuple");
      oper.setBatchSize(100);
    }

    public void runTest(String opId, String[] mapping, int set, boolean isHashMap)
    {
      tupleCount = 0; // reset

      if (isHashMap) {
        oper.setOrderedColumnMapping(mapping);
      }
      else {
        oper.setSimpleColumnMapping(mapping);
      }

      oper.setup(new com.malhartech.engine.OperatorContext(opId, null, null));

      createDatabase(oper.getDbName(), oper.getConnection());
      try {

        createTable(oper.getTableName(), oper.getConnection(), oper.getColumnNames(),
                    isHashMap ? oper.getColumnToType() : oper.simpleColumnToType2);
      }
      catch (Exception ex) {
        throw new RuntimeException("Exception during creating table", ex);
      }

      oper.beginWindow(oper.lastWindowId + 1);
      logger.debug("beginwindow {}", Codec.getStringWindowId(oper.lastWindowId + 1));

      for (int i = 0; i < maxTuple; ++i) {
        oper.inputPort.process(isHashMap ? helper.hashMapData(set, i) : helper.arrayListData(set, i));
      }
      oper.endWindow();
      readTable(oper.getTableName(), oper.getConnection());

      oper.teardown();

      // Check values send vs received
      Assert.assertEquals("Number of emitted tuples", maxTuple, tupleCount);
      logger.debug(String.format("Number of emitted tuples: %d", tupleCount));
    }
  }

  @Test
  public void JDBCHashMapOutputOperatorTest() throws Exception
  {
    dataset = 1;
    JDBCHashMapOutputOperator oper = new JDBCHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op1", helper.hashMapping1, 1, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest2() throws Exception
  {
    dataset = 2;
    JDBCHashMapOutputOperator oper = new JDBCHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    // You can set additional operator parameter here (see below) before calling runTest().
    oper.setBatchSize(6);
    tp.runTest("op2", helper.hashMapping2, 2, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest3() throws Exception
  {
    dataset = 3;
    JDBCHashMapOutputOperator oper = new JDBCHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op3", helper.hashMapping3, 3, true);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest4() throws Exception
  {
    dataset = 3;
    JDBCArrayListOutputOperator oper = new JDBCArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op4", helper.arrayMapping1, 1, false);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest5() throws Exception
  {
    dataset = 3;
    JDBCArrayListOutputOperator oper = new JDBCArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op5", helper.arrayMapping2, 2, false);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest6() throws Exception
  {
    dataset = 3;
    JDBCArrayListOutputOperator oper = new JDBCArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    try {
      tp.runTest("op6", helper.arrayMapping3, 3, false);
    }
    catch (Exception ex) {
      logger.debug("Expected exception for bad column type: ", ex.getLocalizedMessage());
    }
  }

    @Test
  public void JDBCHashMapOutputOperatorTest21() throws Exception
  {
    dataset = 1;
    JDBCHashMapNonTransactionOutputOperator oper = new JDBCHashMapNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op21", helper.hashMapping1, 1, true);
  }

 @Test
  public void JDBCHashMapOutputOperatorTest22() throws Exception
  {
    dataset = 2;
    JDBCHashMapNonTransactionOutputOperator oper = new JDBCHashMapNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    // You can set additional operator parameter here (see below) before calling runTest().
    oper.setBatchSize(6);
    tp.runTest("op22", helper.hashMapping2, 2, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest23() throws Exception
  {
    dataset = 3;
    JDBCHashMapNonTransactionOutputOperator oper = new JDBCHashMapNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op23", helper.hashMapping3, 3, true);
  }

   @Test
  public void JDBCArrayListOutputOperatorTest24() throws Exception
  {
    dataset = 3;
    JDBCArrayListNonTransactionOutputOperator oper = new JDBCArrayListNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op24", helper.arrayMapping1, 1, false);
  }


  @Test
  public void JDBCArrayListOutputOperatorTest25() throws Exception
  {
    dataset = 3;
    JDBCArrayListNonTransactionOutputOperator oper = new JDBCArrayListNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest("op25", helper.arrayMapping2, 2, false);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest26() throws Exception
  {
    dataset = 3;
    JDBCArrayListNonTransactionOutputOperator oper = new JDBCArrayListNonTransactionOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    try {
      tp.runTest("op26", helper.arrayMapping3, 3, false);
    }
    catch (Exception ex) {
      logger.debug("Expected exception for bad column type: ", ex.getLocalizedMessage());
    }
  }
}
