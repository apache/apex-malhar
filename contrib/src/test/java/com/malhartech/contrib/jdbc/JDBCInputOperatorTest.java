/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.jdbc;

import com.malhartech.engine.TestSink;
import java.sql.ResultSet;
import java.sql.SQLException;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCInputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCInputOperatorTest.class);
  private static final int maxTuple = 20;
  private int count = 0;

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

  @Test
  public void JDBCInputOperatorTest() throws Exception
  {
    JDBCOperatorTestHelper helper = new JDBCOperatorTestHelper();
    helper.buildDataset();

    MyJDBCInputOperator oper = new MyJDBCInputOperator();
    oper.setTableName("Test_Tuple");
    oper.setDbUrl("jdbc:mysql://localhost/test?user=test&password=");
    oper.setDbDriver("com.mysql.jdbc.Driver");
    oper.setBatchSize(100);
    oper.setWindowIdColumnName("winid");
    oper.setOperatorIdColumnName("operatorid");
    oper.setApplicationIdColumnName("appid");

    TestSink<String> sink = new TestSink<String>();
    oper.outputPort.setSink(sink);

    oper.setColumnMapping(helper.hashMapping1);

    helper.setupDB(oper, helper.hashMapping1, true, true);
    oper.setup(new com.malhartech.engine.OperatorContext("op1", null, null));
    oper.beginWindow(oper.lastWindowId + 1);
    helper.insertData(helper.hashMapping1, true);
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();
    helper.cleanupDB();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", maxTuple, count);
    logger.debug(String.format("Number of emitted tuples: %d", count));
  }
}
