/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.springframework.integration.jdbc.config;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class JDBCOutputOperator<T> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCOutputOperator.class);
  private String dbUrl;
  private String dbName;
  private String dbUser;
  private String dbPassword;
  private String dbDriver;
  private String tableName;
  private ArrayList<String> orderedColumnMapping = new ArrayList<String>();
  private ArrayList<String> orderedColumns = new ArrayList<String>(); // follow same order as items in tuple
  private HashMap<String, Integer> keyToIndex = new HashMap<String, Integer>();
  private HashMap<String, String> keyToType = new HashMap<String, String>();
  private HashMap<String, String> keyToColumn = new HashMap<String, String>();
  private HashMap<String, String> columnToType = new HashMap<String, String>();
  private HashMap<String, Integer> columnSQLTypes = new HashMap<String, Integer>();
  private Connection connection = null;
  private PreparedStatement insertStatement = null;
  protected long windowId;
  protected long lastWindowId;
  protected boolean ignoreWindow;

  public abstract void processTuple(T tuple);
  /**
   * The input port.
   */
  @InputPortFieldAnnotation(name = "inputPort")
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      if (ignoreWindow) {
        return;
      }
      processTuple(tuple);
    }
  };

  public String getDbUrl()
  {
    return dbUrl;
  }

  public void setDbUrl(String dbUrl)
  {
    this.dbUrl = dbUrl;
  }

  public String getDbName()
  {
    return dbName;
  }

  public void setDbName(String dbName)
  {
    this.dbName = dbName;
  }

  public String getDbUser()
  {
    return dbUser;
  }

  public void setDbUser(String dbUser)
  {
    this.dbUser = dbUser;
  }

  public String getDbPassword()
  {
    return dbPassword;
  }

  public void setDbPassword(String dbPassword)
  {
    this.dbPassword = dbPassword;
  }

  public String getDbDriver()
  {
    return dbDriver;
  }

  public void setDbDriver(String dbDriver)
  {
    this.dbDriver = dbDriver;
  }

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public ArrayList<String> getOrderedColumnMapping()
  {
    return orderedColumnMapping;
  }

  public void setOrderedColumnMapping(String[] orderedColumnMapping)
  {
    if (orderedColumnMapping != null) {
      this.orderedColumnMapping.addAll(Arrays.asList(orderedColumnMapping));
    }
  }

  public Connection getConnection()
  {
    return connection;
  }

  public PreparedStatement getInsertStatement()
  {
    return insertStatement;
  }

  public void setInsertStatement(PreparedStatement insertStatement)
  {
    this.insertStatement = insertStatement;
  }

  public ArrayList<String> getOrderedColumns()
  {
    @Override
    public void process(HashMap<String, V> tuple)
    {
      if( windowId <= lastWindowId ) {
        logger.debug("lastWindowId:"+lastWindowId+" windowId:"+windowId);
        return;
      }
      try {
        for (Map.Entry<String, V> e: tuple.entrySet()) {
          ps.setString(keyToIndex.get(e.getKey()).intValue(), e.getValue().toString());
          count++;
        }
        ps.executeUpdate();
      }
      catch (SQLException ex) {
        logger.debug("exception while update", ex);
      }

  public HashMap<String, String> getKeyToType()
  {
    return keyToType;
  }

  public HashMap<String, String> getKeyToColumn()
  {
    return keyToColumn;
  }

  public HashMap<String, Integer> getKeyToIndex()
  {
    return keyToIndex;
  }

  public HashMap<String, String> getColumnToType()
  {
    return columnToType;
  }

  public HashMap<String, Integer> getColumnSQLTypes()
  {
    return columnSQLTypes;
  }

  public void buildMapping()
  {
    /*  BIGINT
     BINARY
     BIT
     CHAR
     DATE
     DECIMAL
     DOUBLE
     FLOAT
     INTEGER
     LONGVARBINARY
     LONGVARCHAR
     NULL
     NUMERIC
     OTHER
     REAL
     SMALLINT
     TIME
     TIMESTAMP
     TINYINT
     VARBINARY
     VARCHAR */
    columnSQLTypes.put("BIGINT", new Integer(Types.BIGINT));
    columnSQLTypes.put("BINARY", new Integer(Types.BINARY));
    columnSQLTypes.put("BIT", new Integer(Types.BIT));
    columnSQLTypes.put("CHAR", new Integer(Types.CHAR));
    columnSQLTypes.put("DATE", new Integer(Types.DATE));
    columnSQLTypes.put("DECIMAL", new Integer(Types.DECIMAL));
    columnSQLTypes.put("DOUBLE", new Integer(Types.DOUBLE));
    columnSQLTypes.put("FLOAT", new Integer(Types.FLOAT));
    columnSQLTypes.put("INTEGER", new Integer(Types.INTEGER));
    columnSQLTypes.put("LONGVARBINARY", new Integer(Types.LONGVARBINARY));
    columnSQLTypes.put("LONGVARCHAR", new Integer(Types.LONGVARCHAR));
    columnSQLTypes.put("NULL", new Integer(Types.NULL));
    columnSQLTypes.put("NUMERIC", new Integer(Types.NUMERIC));
    columnSQLTypes.put("OTHER", new Integer(Types.OTHER));
    columnSQLTypes.put("REAL", new Integer(Types.REAL));
    columnSQLTypes.put("SMALLINT", new Integer(Types.SMALLINT));
    columnSQLTypes.put("TIME", new Integer(Types.TIME));
    columnSQLTypes.put("TIMESTAMP", new Integer(Types.TIMESTAMP));
    columnSQLTypes.put("TINYINT", new Integer(Types.TINYINT));
    columnSQLTypes.put("VARBINARY", new Integer(Types.VARBINARY));
    columnSQLTypes.put("VARCHAR", new Integer(Types.VARCHAR));

    //JdbcTypesEnum e = new JdbcTypesEnum();

    try {
      // Each entry in orderedColumnMapping is Tuple key followed by Tuple value separated by colon (:)
      int num = orderedColumnMapping.size();
      String delimiter = ":";

      for (int idx = 0; idx < num; ++idx) {
        String[] cols = orderedColumnMapping.get(idx).split(delimiter);
        if (cols.length < 2 || cols.length > 3) {
          throw new Exception("bad column mapping");
        }
        keyToColumn.put(cols[0], cols[1]);
        keyToIndex.put(cols[0], new Integer(idx + 1));
        orderedColumns.add(cols[1]);
        if (cols.length == 3) {
          keyToType.put(cols[0], cols[2].contains("VARCHAR") ? "VARCHAR" : cols[2]);
          columnToType.put(cols[1], cols[2]);
        }
        else {
          keyToType.put(cols[0], "UNSPECIFIED");
          columnToType.put(cols[1], "UNSPECIFIED");
        }
      }
      logger.debug(keyToColumn.toString());
    }
    catch (Exception ex) {
      logger.debug("exception during table column mapping", ex);
    }
  }

  public void setupJDBCConnection()
  {
    try {
      // This will load the MySQL driver, each DB has its own driver
      Class.forName(dbDriver).newInstance();
      connection = DriverManager.getConnection(dbUrl + dbName, dbUser, dbPassword);
      logger.debug("JDBC connection Success");
    }
    catch (ClassNotFoundException ex) {
      logger.debug("exception during JBDC connection", ex);
    }
    catch (Exception ex) {
      logger.debug("exception during JDBC connection", ex);
    }
  }

  /**
   * Prepare insert query statement using column names from mapping.
   *
   */
  protected void prepareInsertStatement()
  {
    int num = orderedColumns.size();
    if (num < 1) {
      return;
    }
    String columns = "";
    String values = "";
    String space = " ";
    String comma = ",";
    String question = "?";

    for (int idx = 0; idx < num; ++idx) {
      if (idx == 0) {
        columns = orderedColumns.get(idx);
        values = question;
      }
      else {
        columns = columns + comma + space + orderedColumns.get(idx);
        values = values + comma + space + question;
      }
    }

    String insertQuery = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")";
    logger.debug(String.format("%s", insertQuery));
    try {
      insertStatement = connection.prepareStatement(insertQuery);
    }
    catch (SQLException ex) {
      logger.debug("exception during prepare statement", ex);
    }
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    buildMapping();
    setupJDBCConnection();
    prepareInsertStatement();
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    try {
      if (insertStatement != null) {
        insertStatement.close();
      }
      connection.close();
    }
    catch (SQLException ex) {
      logger.debug("exception during teardown", ex);
    }
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
  }
}
