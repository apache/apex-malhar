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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;

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
 *
 * @since 0.3.2
 * @deprecated use {@link AbstractJdbcTransactionableOutputOperator}
 */
@Deprecated
public abstract class JDBCOutputOperator<T> extends JDBCOperatorBase implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCOutputOperator.class);
  protected static final int DEFAULT_BATCH_SIZE = 1000;
  protected static final String FIELD_DELIMITER = ":";
  protected static final String COLUMN_DELIMITER = ".";

  // Followings are user input
  // tableName is required only if not mentioned in mapping in case of single table.
  private String tableName = "";
  @Min(1)
  protected long batchSize = DEFAULT_BATCH_SIZE;
  // column mapping is required for output operator.
  @NotNull
  protected ArrayList<String> columnMapping = new ArrayList<String>();
  @NotNull
  protected String windowIdColumnName;
  @NotNull
  protected String operatorIdColumnName;  // Storing operator id is for partitioning purpose
  @NotNull
  protected String applicationIdColumnName;
  // end of user input
  //
  protected transient ArrayList<String> columnNames = new ArrayList<String>();
  protected transient ArrayList<String> tableNames = new ArrayList<String>();
  protected transient HashMap<String, Integer> keyToIndex = new HashMap<String, Integer>();
  protected transient HashMap<String, String> keyToType = new HashMap<String, String>();
  protected transient HashMap<String, String> keyToTable = new HashMap<String, String>();
  protected transient HashMap<String, ArrayList<String>> tableToColumns = new HashMap<String, ArrayList<String>>();
  //
  private transient HashMap<String, Integer> columnSQLTypes = new HashMap<String, Integer>();
  protected transient HashMap<String, PreparedStatement> tableToInsertStatement = new HashMap<String, PreparedStatement>();
  protected transient HashMap<String, String> tableToSelectQuery = new HashMap<String, String>();

  protected transient long windowId;
  protected transient long lastWindowId;
  protected transient String applicationId;
  protected transient boolean ignoreWindow;
  protected transient int operatorId;
  protected long tupleCount = 0;
  protected transient boolean emptyTuple = false;
  // additional variables for arraylist mapping
  protected transient ArrayList<String> tableArray = new ArrayList<String>();
  protected transient ArrayList<String> typeArray = new ArrayList<String>();
  protected transient ArrayList<Integer> columnIndexArray = new ArrayList<Integer>();

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  @Min(1)
  public long getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(long batchSize)
  {
    this.batchSize = batchSize;
  }

  @NotNull
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
  public int getOperatorId()
  {
    return operatorId;
  }

  public void setOperatorId(int operatorId)
  {
    this.operatorId = operatorId;
  }

  @NotNull
  public String getWindowIdColumnName()
  {
    return windowIdColumnName;
  }

  public void setWindowIdColumnName(String sWindowId)
  {
    this.windowIdColumnName = sWindowId;
  }

  @NotNull
  public String getOperatorIdColumnName()
  {
    return operatorIdColumnName;
  }

  public void setOperatorIdColumnName(String sOperatorId)
  {
    this.operatorIdColumnName = sOperatorId;
  }

  @NotNull
  public String getApplicationIdColumnName()
  {
    return applicationIdColumnName;
  }

  public void setApplicationIdColumnName(String sApplicationId)
  {
    this.applicationIdColumnName = sApplicationId;
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

  /**
   * Followings are SQL datatypes that JDBC support. The column mapping has to contain
   * one of these types.
   *   BIGINT
   *   BINARY
   *   BIT
   *   CHAR
   *   DATE
   *   DECIMAL
   *   DOUBLE
   *   FLOAT
   *   INTEGER
   *   INT
   *   LONGVARBINARY
   *   LONGVARCHAR
   *   NULL
   *   NUMERIC
   *   OTHER
   *   REAL
   *   SMALLINT
   *   TIME
   *   TIMESTAMP
   *   TINYINT
   *   VARBINARY
   *   VARCHAR
   *
   */
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

    if (columnMapping.isEmpty()) {
      throw new RuntimeException("Empty column mapping");
    }

    parseMapping(columnMapping);
    logger.debug(keyToIndex.toString());
    logger.debug(keyToType.toString());
  }

    /**
   * Parse column mapping set by the user.
   * Since the mapping is different based on HashMap or ArrayList implement this in concrete derived class.
   * This is needed only for output operator.
   *
   * @param mapping
   */
  protected abstract void parseMapping(ArrayList<String> mapping);

  /**
   * The column mapping will have following pattern: <br>
   * Property:[Table.]Column:Type <br>
   * Followings are two examples: <br>
   * prop1:t1.col1:INTEGER,prop2:t3.col2:BIGINT,prop5:t3.col5:CHAR,prop6:t2.col4:DATE,prop7:t1.col7:DOUBLE,prop3:t2.col6:VARCHAR(10),prop4:t1.col3:DATE <br>
   * prop1:col1:INTEGER,prop2:col2:BIGINT,prop5:col5:CHAR,prop6:col4:DATE,prop7:col7:DOUBLE,prop3:col6:VARCHAR(10),prop4:col3:DATE <br>
   *
   * @param mapping
   */
  public void parseHashMapColumnMapping(ArrayList<String> mapping)
  {
    int propIdx = 0;
    int colIdx = 1;
    int typeIdx = 2;

    int num = mapping.size();
    String table;
    String column;

    for (int idx = 0; idx < num; ++idx) {
      String[] fields = mapping.get(idx).split(FIELD_DELIMITER);
      if (fields.length != 3) {
        throw new RuntimeException("Incorrect column mapping for HashMap. Correct mapping should be \"Property:[Table.]Column:Type\"");
      }

      int colDelIdx = fields[colIdx].indexOf(COLUMN_DELIMITER);
      if (colDelIdx != -1) { // table name is used
        table = fields[colIdx].substring(0, colDelIdx);
        column = fields[colIdx].substring(colDelIdx + 1);
        if (!tableNames.contains(table)) {
          tableNames.add(table);
        }
      }
      else { // table name not used; so this must be single table
        table = getTableName();
        if (table.isEmpty()) {
          throw new RuntimeException("Table name can not be empty");
        }
        if (tableNames.isEmpty()) {
          tableNames.add(table);
        }
        column = fields[colIdx];
      }
      columnNames.add(column);
      keyToTable.put(fields[propIdx], table);

      if (tableToColumns.containsKey(table)) {
        tableToColumns.get(table).add(column);
      }
      else {
        ArrayList<String> cols = new ArrayList<String>();
        cols.add(column);
        tableToColumns.put(table, cols);
      }

      keyToIndex.put(fields[propIdx], tableToColumns.get(table).size());
      keyToType.put(fields[propIdx], fields[typeIdx].toUpperCase().contains("VARCHAR") ? "VARCHAR" : fields[typeIdx].toUpperCase());
    }
  }

  /**
   * The column mapping will have following pattern: <br>
   * [Table.]Column:Type <br>
   * Followings are two examples: <br>
   * t1.col1:INTEGER,t3.col2:BIGINT,t3.col5:CHAR,t2.col4:DATE,t1.col7:DOUBLE,t2.col6:VARCHAR(10),t1.col3:DATE <br>
   * col1:INTEGER,col2:BIGINT,col5:CHAR,col4:DATE,col7:DOUBLE,col6:VARCHAR(10),col3:DATE <br>
   *
   * @param mapping
   */
  public void parseArrayListColumnMapping(ArrayList<String> mapping)
  {
    int colIdx = 0;
    int typeIdx = 1;
    int num = mapping.size();
    String table;
    String column;

    for (int idx = 0; idx < num; ++idx) {
      String[] fields = mapping.get(idx).split(FIELD_DELIMITER);
      if (fields.length != 2) {
        throw new RuntimeException("Incorrect column mapping for ArrayList. Correct mapping should be \"[Table.]Column:Type\"");
      }

      int colDelIdx = fields[colIdx].indexOf(COLUMN_DELIMITER);
      if (colDelIdx != -1) { // table name is used
        table = fields[colIdx].substring(0, colDelIdx);
        column = fields[colIdx].substring(colDelIdx + 1);
        if (!tableNames.contains(table)) {
          tableNames.add(table);
        }
      }
      else { // table name not used; so this must be single table
        table = getTableName();
        if (table.isEmpty()) {
          throw new RuntimeException("Table name can not be empty");
        }
        if (tableNames.isEmpty()) {
          tableNames.add(table);
        }
        column = fields[colIdx];
      }
      columnNames.add(column);
      keyToTable.put(fields[colIdx], table);

      if (tableToColumns.containsKey(table)) {
        tableToColumns.get(table).add(column);
      }
      else {
        ArrayList<String> cols = new ArrayList<String>();
        cols.add(column);
        tableToColumns.put(table, cols);
      }

      keyToIndex.put(fields[colIdx], tableToColumns.get(table).size());
      columnIndexArray.add(tableToColumns.get(table).size());
      tableArray.add(table);

      keyToType.put(fields[colIdx], fields[typeIdx].toUpperCase().contains("VARCHAR") ? "VARCHAR" : fields[typeIdx].toUpperCase());
      typeArray.add(fields[typeIdx].toUpperCase().contains("VARCHAR") ? "VARCHAR" : fields[typeIdx].toUpperCase());
    }
  }

  /**
   * Bind tuple values with SQL prepared statement to be inserted to database based on tuple type.
   * Tuple can be HashMap or ArrayList or any other Java object datatype.
   *
   * @param tuple
   * @throws SQLException
   */
  public abstract void processTuple(T tuple) throws SQLException;

  /**
   * The input port.
   */
  @InputPortFieldAnnotation(name = "inputPort")
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    /**
     * Write a batch of tuple into database. The batch size can be configured by the user in batchSize variable.
     */
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
        tableToInsertStatement.put(entry.getKey(), connection.prepareStatement(insertQuery));
      }
      catch (SQLException ex) {
        throw new RuntimeException(String.format("Error while preparing insert query: %s", insertQuery), ex);
      }
    }
  }

  /**
   * This is the place to have initial setup for the operator. This creates JDBC connection and prepare insert statement for database write.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    buildMapping();
    connect();
    prepareInsertStatement();
    setOperatorId(context.getId());
    applicationId = context.getAttributes().get(DAG.APPLICATION_ID);
  }

  /**
   * Write to database any remaining tuple and close JDBC connection.
   */
  @Override
  public void teardown()
  {
    try {
      for (Map.Entry<String, PreparedStatement> entry: tableToInsertStatement.entrySet()) {
        entry.getValue().close();
      }
    }
    catch (SQLException ex) {
      throw new RuntimeException("Error while closing database resource", ex);
    }
    disconnect();
  }

  /**
   * Do things needed for beginning of window.
   */
  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  /**
   * Write any remaining tuples before closing window.
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
