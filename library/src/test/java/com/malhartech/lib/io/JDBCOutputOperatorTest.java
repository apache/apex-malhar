/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.bufferserver.util.Codec;
import java.sql.*;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
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
  private static int columnCount = 7;
  private static int dataset = 1;
  private static long testWindowId = 0;

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
      logger.debug("exception during creating database", ex);
    }
    finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex) {
      }
    }

    logger.debug("JDBC DB creation Success");
  }

  public static void createTable(String tableName, Connection con, ArrayList<String> columns, HashMap<String, String> colTypes) throws Exception
  {
    int num = columns.size();
    String cols = columns.get(0) + " " + colTypes.get(columns.get(0));
    for (int i = 1; i < num; i++) {
      cols = cols + ", " + columns.get(i) + " " + colTypes.get(columns.get(i));
    }

    String str = "CREATE TABLE " + tableName + " (" + cols + ")";

    Statement stmt = null;
    try {
      stmt = con.createStatement();
      stmt.execute("DROP TABLE IF EXISTS " + tableName);
      stmt.executeUpdate(str);

      // Read maxWindowId
      DatabaseMetaData meta = con.getMetaData();
      ResultSet rs1 = meta.getTables(null, null, "maxwindowid", null);
      if (rs1.next() == true) {
        String querySQL = "SELECT winid FROM maxwindowid LIMIT 1";
        ResultSet rs = stmt.executeQuery(querySQL);
        if (rs.next() == false) {
          logger.error("maxwindowid table is empty");
          throw new Exception();
        }
        testWindowId = rs.getLong("winid");
      }
      else {
        logger.debug("maxwindowid table not exist!");
        throw new Exception();
      }
    }
    catch (SQLException ex) {
      logger.debug("exception during creating database", ex);
    }
    finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex) {
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
      logger.debug("exception during reading from table", ex);
    }
    finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex) {
      }
    }
  }

  public static class MyHashMapOutputOperator extends JDBCHashMapOutputOperator<Integer>
  {
    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      createDatabase(getDbName(), getConnection());
      try {
        createTable(getTableName(), getConnection(), getOrderedColumns(), getColumnToType());
      }
      catch (Exception ex) {
        logger.debug("Exception during setup: create table: %s", ex.getLocalizedMessage());
      }
    }

    @Override
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      logger.debug("beginwindow {}", Codec.getStringWindowId(windowId));
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
      readTable(getTableName(), getConnection());
    }
  }

  @Test
  public void JDBCHashMapOutputOperatorTest() throws Exception
  {
    tupleCount = 0; // reset
    dataset = 1;
    MyHashMapOutputOperator oper = new MyHashMapOutputOperator();

    oper.setDbUrl("jdbc:mysql://localhost/");
    oper.setDbName("test");
    oper.setDbUser("test");
    oper.setDbPassword("");
    oper.setDbDriver("com.mysql.jdbc.Driver");
    oper.setTableName("Test_Tuple");
    String[] mapping = new String[7];
    mapping[0] = "prop1:col1:INTEGER";
    mapping[1] = "prop2:col2:INTEGER";
    mapping[2] = "prop5:col5:INTEGER";
    mapping[3] = "prop6:col4:INTEGER";
    mapping[4] = "prop7:col7:INTEGER";
    mapping[5] = "prop3:col6:INTEGER";
    mapping[6] = "prop4:col3:INTEGER";
    oper.setOrderedColumnMapping(mapping);
    oper.setBatchSize(100);

    //oper.setColumnMapping("prop1:col1,prop2:col2,prop5:col5,prop6:col6,prop7:col7,prop3:col3,prop4:col4");
    //columnMapping=prop1:col1,prop2:col2,prop3:col3,prop4:col4,prop5:col5,prop6:col6,prop7:col7
    //columnMapping=prop1:col1,prop2:col2,prop5:col5,prop6:col6,prop7:col7,prop3:col3,prop4:col4
    ///columnMapping=prop1:col1,prop2:col2,prop5:col5,prop6:col4,prop7:col7,prop3:col6,prop4:col3

    oper.setup(new com.malhartech.engine.OperatorContext("op1", null, null));
    oper.beginWindow(testWindowId+1);
    for (int i = 0; i < maxTuple; ++i) {
      HashMap<String, Integer> hm = new HashMap<String, Integer>();
      for (int j = 1; j <= columnCount; ++j) {
        hm.put("prop" + (j), new Integer((columnCount * i) + j));
      }
      oper.inputPort.process(hm);
    }
    oper.endWindow();

    oper.teardown();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", maxTuple, tupleCount);
    logger.debug(String.format("Number of emitted tuples: %d", tupleCount));
  }

  public static class MyArrayListOutputOperator extends JDBCArrayListOutputOperator
  {
    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      try {
        //      createDatabase(getDbName(), getConnection());
              createTable(getTableName(), getConnection(), getOrderedColumns(), getColumnToType());
      }
      catch (Exception ex) {
        logger.debug("exception while update", ex);
      }
    }

    @Override
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      logger.debug("beginwindow {}", Codec.getStringWindowId(windowId));
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
      readTable(getTableName(), getConnection());
    }
  }

  @Test
  public void JDBCArrayListOutputOperatorTest() throws Exception
  {
    tupleCount = 0; // reset
    dataset = 2;
    MyArrayListOutputOperator oper = new MyArrayListOutputOperator();

    oper.setDbUrl("jdbc:mysql://localhost/");
    oper.setDbName("test");
    oper.setDbUser("test");
    oper.setDbPassword("");
    oper.setDbDriver("com.mysql.jdbc.Driver");
    oper.setTableName("Test_Tuple");
    String[] mapping = new String[7];
    mapping[0] = "prop1:col1:INTEGER";
    mapping[1] = "prop2:col2:VARCHAR(10)";
    mapping[2] = "prop5:col5:INTEGER";
    mapping[3] = "prop6:col4:VARCHAR(10)";
    mapping[4] = "prop7:col7:INTEGER";
    mapping[5] = "prop3:col6:VARCHAR(10)";
    mapping[6] = "prop4:col3:INTEGER";
    oper.setOrderedColumnMapping(mapping);

    oper.setup(new com.malhartech.engine.OperatorContext("op2", null, null));
    oper.beginWindow(testWindowId+1);
    for (int i = 0; i < maxTuple; ++i) {
      ArrayList<AbstractMap.SimpleEntry<String, Object>> al = new ArrayList<AbstractMap.SimpleEntry<String, Object>>();
      for (int j = 1; j <= columnCount; ++j) {
        if ("INTEGER".equals(oper.getKeyToType().get("prop" + j))) {
          al.add(new AbstractMap.SimpleEntry<String, Object>("prop" + j, new Integer(columnCount * i + j)));
        }
        else {
          al.add(new AbstractMap.SimpleEntry<String, Object>("prop" + j, "Test"));
        }
      }

      oper.inputPort.process(al);
    }
    oper.endWindow();

    oper.teardown();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", maxTuple, tupleCount);
    logger.debug(String.format("Number of emitted tuples: %d", tupleCount));
  }

  @Test
  public void JDBCArrayListOutputOperator_multiType_Test() throws Exception
  {
    tupleCount = 0; // reset
    dataset = 3;
    MyArrayListOutputOperator oper = new MyArrayListOutputOperator();

    oper.setDbUrl("jdbc:mysql://localhost/");
    oper.setDbName("test");
    oper.setDbUser("test");
    oper.setDbPassword("");
    oper.setDbDriver("com.mysql.jdbc.Driver");
    oper.setTableName("Test_Tuple");
    String[] mapping = new String[7];
    mapping[0] = "prop1:col1:INTEGER";
    mapping[1] = "prop2:col2:BIGINT";
    mapping[2] = "prop5:col5:CHAR";
    mapping[3] = "prop6:col4:DATE";
    mapping[4] = "prop7:col7:DOUBLE";
    mapping[5] = "prop3:col6:VARCHAR(10)";
    mapping[6] = "prop4:col3:DATE";
    oper.setOrderedColumnMapping(mapping);

    oper.setup(new com.malhartech.engine.OperatorContext("op3", null, null));
    oper.beginWindow(testWindowId+1);
    for (int i = 0; i < maxTuple; ++i) {
      ArrayList<AbstractMap.SimpleEntry<String, Object>> al = new ArrayList<AbstractMap.SimpleEntry<String, Object>>();
      for (int j = 1; j <= columnCount; ++j) {
        if ("INTEGER".equals(oper.getKeyToType().get("prop" + j))) {
          al.add(new AbstractMap.SimpleEntry<String, Object>("prop" + j, new Integer(columnCount * i + j)));
        }
        else if ("BIGINT".equals(oper.getKeyToType().get("prop" + j))) {
          al.add(new AbstractMap.SimpleEntry<String, Object>("prop" + j, new Integer(columnCount * i + j)));
        }
        else if ("CHAR".equals(oper.getKeyToType().get("prop" + j))) {
          al.add(new AbstractMap.SimpleEntry<String, Object>("prop" + j, 'a'));
        }
        else if ("DATE".equals(oper.getKeyToType().get("prop" + j))) {
          al.add(new AbstractMap.SimpleEntry<String, Object>("prop" + j, new Date()));
        }
        else if ("DOUBLE".equals(oper.getKeyToType().get("prop" + j))) {
          al.add(new AbstractMap.SimpleEntry<String, Object>("prop" + j, new Double((columnCount * i + j) / 3.0)));
        }
        else if ("VARCHAR".equals(oper.getKeyToType().get("prop" + j))) {
          al.add(new AbstractMap.SimpleEntry<String, Object>("prop" + j, "Test"));
        }
        else if ("TIME".equals(oper.getKeyToType().get("prop" + j))) {
          al.add(new AbstractMap.SimpleEntry<String, Object>("prop" + j, new Date()));
        }
        else {
          throw new Exception();
        }
      }

      oper.inputPort.process(al);
    }
    oper.endWindow();

    oper.teardown();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", maxTuple, tupleCount);
    logger.debug(String.format("Number of emitted tuples: %d", tupleCount));
  }
}
