/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.jdbc;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.Min;
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
  protected static final String FIELD_DELIMITER = ":";
  protected static final String COLUMN_DELIMITER = ".";
  //
  // Followings are  user input
  @NotNull
  private String dbUrl;
  @NotNull
  private String dbDriver;
  // tableName is required only if not mentioned in mapping in case of single table.
  private String tableName = "";
  @Min(1)
  private long batchSize = DEFAULT_BATCH_SIZE;
  @NotNull
  private ArrayList<String> columnMapping = new ArrayList<String>();
  // end of user input
  //
  protected transient ArrayList<String> columnNames = new ArrayList<String>(); // follow same order as items in tuple; could be table.column
  protected transient ArrayList<String> tableNames = new ArrayList<String>();
  protected transient HashMap<String, Integer> keyToIndex = new HashMap<String, Integer>();
  protected transient HashMap<String, String> keyToType = new HashMap<String, String>();
  protected transient HashMap<String, String> keyToTable = new HashMap<String, String>();
  protected transient HashMap<String, ArrayList<String>> tableToColumns = new HashMap<String, ArrayList<String>>();
  private transient HashMap<String, Integer> columnSQLTypes = new HashMap<String, Integer>();
  protected transient HashMap<String, PreparedStatement> tableToInsertStatement = new HashMap<String, PreparedStatement>();
  private transient Connection connection = null;
  protected transient long windowId;
  protected transient long lastWindowId;
  protected transient boolean ignoreWindow;
  protected transient String operatorId;

  protected String sWindowId;
  protected String sOperatorId;  // Storing operator id is for partitioning purpose
  protected String sApplicationId;
  private long tupleCount = 0;
  protected transient boolean emptyTuple = false;

  // additional variables for arraylist mapping
  protected transient ArrayList<String> tableArray = new ArrayList<String>();
  protected transient ArrayList<String> typeArray = new ArrayList<String>();
  protected transient ArrayList<Integer> columnIndexArray = new ArrayList<Integer>();

  /**
   * Parse column mapping set by the user.
   * Since the mapping is different based on HashMap or ArrayList do this in concrete derived class.
   *
   * @param mapping
   */
  protected abstract void parseMapping(ArrayList<String> mapping);

  /**
   * Implement how to process tuple in derived class based on HashMap or ArrayList.
   * The tuple values are binded with SQL prepared statement to be inserted to database.
   *
   * @param tuple
   * @throws SQLException
   */
  public abstract void processTuple(T tuple) throws SQLException;
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
        ++tupleCount;
        for (Map.Entry<String, PreparedStatement> entry: tableToInsertStatement.entrySet()) {
          entry.getValue().addBatch();
          if (tupleCount % batchSize == 0) {
            entry.getValue().executeBatch();
          }
        }
      }
      catch (SQLException ex) {
        throw new RuntimeException(String.format("Unable to insert data during process"), ex);
      }
      catch (Exception ex) {
        throw new RuntimeException("Exception during process tuple", ex);
      }
      //logger.debug(String.format("generated tuple count so far: %d", tupleCount));
    }
  };

  public String getOperatorId()
  {
    return operatorId;
  }

  public void setOperatorId(String operatorId)
  {
    this.operatorId = operatorId;
  }

  public String getsWindowId()
  {
    return sWindowId;
  }

  public void setsWindowId(String sWindowId)
  {
    this.sWindowId = sWindowId;
  }

  public String getsOperatorId()
  {
    return sOperatorId;
  }

  public void setsOperatorId(String sOperatorId)
  {
    this.sOperatorId = sOperatorId;
  }

  public String getsApplicationId()
  {
    return sApplicationId;
  }

  public void setsApplicationId(String sApplicationId)
  {
    this.sApplicationId = sApplicationId;
  }

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

  public ArrayList<String> getColumnMapping()
  {
    return columnMapping;
  }

  public void setColumnMapping(String[] columnMapping)
  {
    if (columnMapping != null) {
      this.columnMapping.addAll(Arrays.asList(columnMapping));
    }
  }

  public Connection getConnection()
  {
    return connection;
  }

  public ArrayList<String> getColumnNames()
  {
    return columnNames;
  }

  public long getTupleCount()
  {
    return tupleCount;
  }

  public int getSQLColumnType(String type)
  {
    int SQLType = 0;
    try {
      SQLType = columnSQLTypes.get(type);
    }
    catch (Exception ex) {
      throw new RuntimeException(String.format("Unsupported SQL type %s in column mapping.", type), ex);
    }
    return SQLType;
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
    columnSQLTypes.put("INT", new Integer(Types.INTEGER));
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

    if (columnMapping.isEmpty()) { // can be validation check
      throw new RuntimeException("Empty column mapping");
    }

    parseMapping(columnMapping);
    logger.debug(keyToIndex.toString());
    logger.debug(keyToType.toString());
  }

  public void setupJDBCConnection()
  {
    try {
      // This will load the JDBC driver, each DB has its own driver
      Class.forName(dbDriver).newInstance();
      connection = DriverManager.getConnection(dbUrl);

      logger.debug("JDBC connection Success");
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException("Exception during JBDC connection", ex);
    }
    catch (Exception ex) {
      throw new RuntimeException("Exception during JBDC connection", ex);
    }
  }

  /**
   * Additional column names, needed for non-transactional database.
   *
   * @return
   */
  protected ArrayList<String> windowColumn()
  {
    return null;
  }

  /**
   * Prepare insert query statement using column names from mapping.
   *
   */
  protected void prepareInsertStatement()
  {
    if (tableToColumns.isEmpty()) {
      return;
    }

    String space = " ";
    String comma = ",";
    String question = "?";

    for (Map.Entry<String, ArrayList<String>> entry: tableToColumns.entrySet()) {
      //int num = columnNames.size();
      int num = entry.getValue().size();
      if (num < 1) {
        return;
      }
      String columns = "";
      String values = "";


      for (int idx = 0; idx < num; ++idx) {
        if (idx == 0) {
          columns = entry.getValue().get(idx);
          values = question;
        }
        else {
          columns += comma + space + entry.getValue().get(idx);
          values += comma + space + question;
        }
      }

      ArrayList<String> windowCol = windowColumn();
      if (windowCol != null && windowCol.size() > 0) {
        for (int i =0; i<windowCol.size(); i++) {
          columns += comma + space + windowCol.get(i);
          values += comma + space + question;
        }
      }

      String insertQuery = "INSERT INTO " + entry.getKey() + " (" + columns + ") VALUES (" + values + ")";
      logger.debug(String.format("%s", insertQuery));
      try {
        //insertStatement = connection.prepareStatement(insertQuery);
        tableToInsertStatement.put(entry.getKey(), connection.prepareStatement(insertQuery));
      }
      catch (SQLException ex) {
        throw new RuntimeException(String.format("Error while preparing insert query: %s", insertQuery), ex);
      }
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
    setOperatorId(context.getId());
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    try {
      for (Map.Entry<String, PreparedStatement> entry: tableToInsertStatement.entrySet()) {
        entry.getValue().close();
      }
      connection.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException("Error while closing database resource", ex);
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
      for (Map.Entry<String, PreparedStatement> entry: tableToInsertStatement.entrySet()) {
        entry.getValue().executeBatch();
      }
    }
    catch (SQLException ex) {
      throw new RuntimeException("Unable to insert data while in endWindow", ex);
    }
  }
}
