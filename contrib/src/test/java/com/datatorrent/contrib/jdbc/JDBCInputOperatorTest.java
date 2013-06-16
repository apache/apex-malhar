/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.jdbc;

import com.datatorrent.contrib.jdbc.JDBCInputOperator;
import com.datatorrent.engine.OperatorContext;
import com.datatorrent.engine.TestSink;

import java.sql.*;
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

    TestSink sink = new TestSink();
    oper.outputPort.setSink(sink);

    setupDB(helper.hashMapping1);

    //oper.setup(new OperatorContext(111, null, null, helper.attrmap));
    oper.setup(new OperatorContext(111, null, null, null));
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
