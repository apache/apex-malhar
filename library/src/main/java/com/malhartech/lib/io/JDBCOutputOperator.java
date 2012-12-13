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
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class JDBCOutputOperator<T> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCOutputOperator.class);
  private static final int DEFAULT_BATCH_SIZE = 1000;
  @NotNull
  private String dbUrl;
  @NotNull
  private String dbName;
  @NotNull
  private String dbUser;
  @NotNull
  private String dbPassword;
  @NotNull
  private String dbDriver;
  @NotNull
  private String tableName;
  @NotNull
  private long batchSize = DEFAULT_BATCH_SIZE;
  private ArrayList<String> orderedColumnMapping = new ArrayList<String>();
  private ArrayList<String> columnNames = new ArrayList<String>(); // follow same order as items in tuple
  private HashMap<String, Integer> keyToIndex = new HashMap<String, Integer>();
  private HashMap<String, String> keyToType = new HashMap<String, String>();
  private HashMap<String, String> keyToColumn = new HashMap<String, String>();
  private HashMap<String, String> columnToType = new HashMap<String, String>();
  private HashMap<String, Integer> columnSQLTypes = new HashMap<String, Integer>();
  private ArrayList<String> simpleColumnMapping = new ArrayList<String>();
  private HashMap<String, String> simpleColumnToType = new HashMap<String, String>();
  public HashMap<String, String> simpleColumnToType2 = new HashMap<String, String>();
  private Connection connection = null;
  private PreparedStatement insertStatement = null;
  protected long windowId;
  protected long lastWindowId;
  protected boolean ignoreWindow;
  private long tupleCount = 0;
  protected boolean emptyTuple = false;
  private boolean hashMapping = true;

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
      if (ignoreWindow || emptyTuple) {
        return; // ignore
      }

      try {
        processTuple(tuple);
        insertStatement.addBatch();
        if (++tupleCount % batchSize == 0) {
          insertStatement.executeBatch();
        }
      }
      catch (SQLException ex) {
        logger.debug("exception during insert tuple", ex);
      }
      logger.debug(String.format("count %d", tupleCount));
    }
  };

  @NotNull
  public String getDbUrl()
  {
    return dbUrl;
  }

  public void setDbUrl(String dbUrl)
  {
    this.dbUrl = dbUrl;
  }

  @NotNull
  public String getDbName()
  {
    return dbName;
  }

  public void setDbName(String dbName)
  {
    this.dbName = dbName;
  }

  @NotNull
  public String getDbUser()
  {
    return dbUser;
  }

  public void setDbUser(String dbUser)
  {
    this.dbUser = dbUser;
  }

  @NotNull
  public String getDbPassword()
  {
    return dbPassword;
  }

  public void setDbPassword(String dbPassword)
  {
    this.dbPassword = dbPassword;
  }

  @NotNull
  public String getDbDriver()
  {
    return dbDriver;
  }

  public void setDbDriver(String dbDriver)
  {
    this.dbDriver = dbDriver;
  }

  @NotNull
  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public long getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(long batchSize)
  {
    this.batchSize = batchSize;
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

  public ArrayList<String> getSimpleColumnMapping()
  {
    return simpleColumnMapping;
  }

  public void setSimpleColumnMapping(String[] simpleColumnMapping)
  {
    if (simpleColumnMapping != null) {
      this.simpleColumnMapping.addAll(Arrays.asList(simpleColumnMapping));
    }
  }

  public HashMap<String, String> getSimpleColumnToType()
  {
    return simpleColumnToType;
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

  public ArrayList<String> getColumnNames()
  {
    return columnNames;
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
    // JDBC SQL type data mapping
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

    try {
      // Each entry in orderedColumnMapping is Tuple key followed by Tuple value separated by colon (:)
      int num = orderedColumnMapping.size();
      String delimiter = ":";

      if (num > 0) {
        hashMapping = true;
        for (int idx = 0; idx < num; ++idx) {
          String[] cols = orderedColumnMapping.get(idx).split(delimiter);
          if (cols.length < 2 || cols.length > 3) {
            throw new Exception("bad column mapping");
          }
          keyToColumn.put(cols[0], cols[1]);
          keyToIndex.put(cols[0], new Integer(idx + 1));
          columnNames.add(cols[1]);
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
      else {
        hashMapping = false;
        int num2 = simpleColumnMapping.size();
        for (int idx = 0; idx < num2; ++idx) {
          String[] cols = simpleColumnMapping.get(idx).split(delimiter);
          columnNames.add(cols[0]);
          if (cols.length != 2) {
            throw new Exception("bad column mapping");
          }
          simpleColumnToType.put(cols[0], cols[1].contains("VARCHAR") ? "VARCHAR" : cols[1]);
          simpleColumnToType2.put(cols[0], cols[1]);
        }
        logger.debug(simpleColumnToType.toString());
      }
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
    int num = columnNames.size();
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
        columns = columnNames.get(idx);
        values = question;
      }
      else {
        columns = columns + comma + space + columnNames.get(idx);
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
    try {
      if (ignoreWindow) {
        return;
      }
      insertStatement.executeBatch();
    }
    catch (SQLException ex) {
      logger.debug("exception during executing batch", ex);
    }
  }
}
