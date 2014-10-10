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


import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 *
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

      oper.setWindowIdColumnName("window_id");
      oper.setOperatorIdColumnName("operator_id");
      oper.setApplicationIdColumnName("application_id");
    }

    public void runTest(int opId, String[] mapping, boolean isHashMap, boolean transaction)
    {
      oper.setColumnMapping(mapping);

      helper.setupDB(oper, mapping, isHashMap, transaction);
      oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(opId));

      oper.beginWindow(oper.lastWindowId + 1);
      logger.debug("beginwindow {}", oper.lastWindowId + 1);

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
    JDBCTransactionHashMapOutputOperator oper = new JDBCTransactionHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest(1, helper.hashMapping1, true, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest2() throws Exception
  {
    JDBCTransactionHashMapOutputOperator oper = new JDBCTransactionHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    // You can set additional operator parameter here (see below) before calling runTest().
    oper.setBatchSize(6);
    oper.setTableName("Test_Tuple");
    tp.runTest(2, helper.hashMapping2, true, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest3() throws Exception
  {
    JDBCTransactionHashMapOutputOperator oper = new JDBCTransactionHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest(3, helper.hashMapping3, true, true);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest4() throws Exception
  {
    JDBCTransactionArrayListOutputOperator oper = new JDBCTransactionArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest(4, helper.arrayMapping1, false, true);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest5() throws Exception
  {
    JDBCTransactionArrayListOutputOperator oper = new JDBCTransactionArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    oper.setTableName("Test_Tuple");
    try {
      tp.runTest(5, helper.arrayMapping2, false, true);
      Assert.assertFalse("This test failed if it ever comes to this line", true);
    }
    catch (Exception ex) {
      logger.debug("Expected exception for bad column type: ", ex.getLocalizedMessage());
    }
  }

  @Test
  public void JDBCArrayListOutputOperatorTest6() throws Exception
  {
    JDBCTransactionArrayListOutputOperator oper = new JDBCTransactionArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest(6, helper.arrayMapping3, false, true);

  }

  // Section for non-transaction operators
  @Test
  public void JDBCHashMapOutputOperatorTest21() throws Exception
  {
    JDBCNonTransactionHashMapOutputOperator oper = new JDBCNonTransactionHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest(21, helper.hashMapping1, true, false);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest22() throws Exception
  {
    JDBCNonTransactionHashMapOutputOperator oper = new JDBCNonTransactionHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    // You can set additional operator parameter here (see below) before calling runTest().
    oper.setBatchSize(6);
    oper.setTableName("Test_Tuple");
    tp.runTest(22, helper.hashMapping2, true, false);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest23() throws Exception
  {
    JDBCNonTransactionHashMapOutputOperator oper = new JDBCNonTransactionHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest(23, helper.hashMapping3, true, false);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest24() throws Exception
  {
    JDBCNonTransactionArrayListOutputOperator oper = new JDBCNonTransactionArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest(24, helper.arrayMapping1, false, false);
  }

  @Test
  public void JDBCArrayListOutputOperatorTest25() throws Exception
  {
    JDBCNonTransactionArrayListOutputOperator oper = new JDBCNonTransactionArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    oper.setTableName("Test_Tuple");
    try {
      tp.runTest(25, helper.arrayMapping2, false, false);
      Assert.assertFalse("This test failed if it ever comes to this line", true);
    }
    catch (Exception ex) {
      logger.debug("Expected exception for bad column type: ", ex.getLocalizedMessage());
    }
  }

  @Test
  public void JDBCArrayListOutputOperatorTest26() throws Exception
  {
    JDBCNonTransactionArrayListOutputOperator oper = new JDBCNonTransactionArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    oper.setTableName("Test_Tuple");
    tp.runTest(26, helper.arrayMapping3, false, false);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest31() throws Exception
  {
    JDBCTransactionHashMapOutputOperator oper = new JDBCTransactionHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    // For mulit table mapping you don't need to set table name
    tp.runTest(31, helper.hashMapping4, true, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest32() throws Exception
  {
    JDBCTransactionHashMapOutputOperator oper = new JDBCTransactionHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    tp.runTest(32, helper.hashMapping5, true, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest33() throws Exception
  {
    JDBCNonTransactionHashMapOutputOperator oper = new JDBCNonTransactionHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest(33, helper.hashMapping4, true, false);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest34() throws Exception
  {
    JDBCNonTransactionHashMapOutputOperator oper = new JDBCNonTransactionHashMapOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest(34, helper.hashMapping5, true, false);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest41() throws Exception
  {
    JDBCTransactionArrayListOutputOperator oper = new JDBCTransactionArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    tp.runTest(41, helper.arrayMapping4, false, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest42() throws Exception
  {
    JDBCTransactionArrayListOutputOperator oper = new JDBCTransactionArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    transactionOperatorSetting(oper);
    tp.runTest(42, helper.arrayMapping5, false, true);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest43() throws Exception
  {
    JDBCNonTransactionArrayListOutputOperator oper = new JDBCNonTransactionArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest(43, helper.arrayMapping4, false, false);
  }

  @Test
  public void JDBCHashMapOutputOperatorTest44() throws Exception
  {
    JDBCNonTransactionArrayListOutputOperator oper = new JDBCNonTransactionArrayListOutputOperator();
    JDBCOutputOperatorTestTemplate tp = new JDBCOutputOperatorTestTemplate(oper);
    tp.runTest(44, helper.arrayMapping5, false, false);
  }
}
