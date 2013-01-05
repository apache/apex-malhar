/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.jdbc;

import com.malhartech.bufferserver.util.Codec;
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
   */


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

      oper.setWindowIdColumnName("winid");
      oper.setOperatorIdColumnName("operatorid");
      oper.setApplicationIdColumnName("appid");
    }

    public void runTest(String opId, String[] mapping, boolean isHashMap)
    {
      oper.setColumnMapping(mapping);

      helper.setupDB(oper, mapping, isHashMap);
      oper.setup(new com.malhartech.engine.OperatorContext(opId, null, null));
      oper.beginWindow(oper.lastWindowId + 1);
      logger.debug("beginwindow {}", Codec.getStringWindowId(oper.lastWindowId + 1));

      for (int i = 0; i < maxTuple; ++i) {
        oper.inputPort.process(isHashMap ? helper.hashMapData(mapping, i) : helper.arrayListData(mapping, i));
      }
      oper.endWindow();
      helper.readDB(oper.getTableName(), mapping, isHashMap);

      oper.teardown();
      helper.cleanupDB();

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
