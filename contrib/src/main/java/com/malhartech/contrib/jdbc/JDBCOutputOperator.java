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
 * JDBC output adapter operator, which writes data into persistence database through JAVA DataBase Connectivity (JDBC) API
 * from Malhar streaming framework.<p><br>
 * Ports:<br>
 * <b>Input</b>: This has a single input port that writes data into database.<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * This is an abstract class. Class derived from this has to implement parseMapping() and processTuple() abstract methods.<br>
 * <br>
 * Run time checks:<br>
 * Following parameters have to be set while using this operator.<br>
 * dbUrl: URL to the database that this operator is going to write. This can not be null.<br>
 * dbDriver: JDBC driver for the database. This can not be null.<br>
 * tableName: If this adapter is writing only to a single table, table name has to be set here unless it is mentioned in column mapping.<br>
 * For writing to multiple table this field is ignored as the table names have to be specified in column mapping. See Column mapping field below for details.<br>
 * batchSize: This has to be at least 1 or more. If not specified the default batch size is 1000.<br>
 * columnMapping: This specifies what data field will be written to which column or which table in the database.This can not be null.<br>
 * The mapping will have following pattern.<br>
 * [Property:][Table.]Column:Type<br>
 * The mapping has three fields separated by colon.<br>
 * 1) Property: If you send key-value pair in the tuple this will be exactly same as key. On the other hand, if you send only value in the tuple<br>
 * this field has to be empty and tuples will be written to database the same order they are received. This field is case-sensitive.<br>
 * 2) Table Column name: Table name and column name has to be separated by dot. If you are writing to a single table, you can omit the table name and<br>
 * just specify the column name only. However in this case  you have to set tableName parameter to specify the name of the table in the database.<br>
 * In case of multi table, you have to specify table name along with column name.This field is case-sensitive.<br>
 * 3) SQL datatype of the column: It only allows the datatype supported by JDBC. The SQL datatype is not case sensitive.<br>
 * <br>
 * Each property, table column, SQL datatype group will be separated by comma. Total number of groups will be same as the number entries in the tuple.<br>
 * <br>
 * Some examples of column mapping:<br>
 * For multi table key-value pair:<br>
 * prop1:t1.col1:INTEGER,prop2:t3.col2:BIGINT,prop5:t3.col5:CHAR,prop6:t2.col4:DATE,prop7:t1.col7:DOUBLE,prop3:t2.col6:VARCHAR(10),prop4:t1.col3:DATE<br>
 * For multi table array list:<br>
 * t1.col1:INTEGER,t3.col2:BIGINT,t3.col5:CHAR,t2.col4:DATE,t1.col7:DOUBLE,t2.col6:VARCHAR(10),t1.col3:DATE<br>
 * For single table key-value pair:<br>
 * prop1:t1.col1:INTEGER,prop2:t1.col2:BIGINT,prop5:t1.col5:CHAR,prop6:t1.col4:DATE,prop7:t1.col7:DOUBLE,prop3:t1.col6:VARCHAR(10),prop4:t1.col3:DATE<br>
 * prop1:col1:INTEGER,prop2:col2:BIGINT,prop5:col5:CHAR,prop6:col4:DATE,prop7:col7:DOUBLE,prop3:col6:VARCHAR(10),prop4:col3:DATE<br>
 * For single table array list:<br>
 * t1.col1:INTEGER,t1.col2:BIGINT,t1.col5:CHAR,t1.col4:DATE,t1.col7:DOUBLE,t1.col6:VARCHAR(10),t1.col3:DATE<br>
 * col1:INTEGER,col2:BIGINT,col5:CHAR,col4:DATE,col7:DOUBLE,col6:VARCHAR(10),col3:DATE<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
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
  protected transient ArrayList<String> columnNames = new ArrayList<String>();
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
   * Since the mapping is different based on HashMap or ArrayList implement this in concrete derived class.
   *
   * @param mapping
   */
  protected abstract void parseMapping(ArrayList<String> mapping);

  /**
   * Implement how to process tuple in derived class based on HashMap or ArrayList column mapping.
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
   * @return array list of column names.
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
