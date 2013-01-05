/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class JDBCOperatorBase
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCOperatorBase.class);
  protected static final int DEFAULT_BATCH_SIZE = 1000;
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
  protected long batchSize = DEFAULT_BATCH_SIZE;
  protected ArrayList<String> columnMapping = new ArrayList<String>();
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
  protected transient Connection connection = null;
  protected transient long windowId;
  protected transient long lastWindowId;
  protected transient boolean ignoreWindow;
  protected transient String operatorId;
  protected String sWindowId;
  protected String sOperatorId;  // Storing operator id is for partitioning purpose
  protected String sApplicationId;
  protected long tupleCount = 0;
  protected transient boolean emptyTuple = false;
  // additional variables for arraylist mapping
  protected transient ArrayList<String> tableArray = new ArrayList<String>();
  protected transient ArrayList<String> typeArray = new ArrayList<String>();
  protected transient ArrayList<Integer> columnIndexArray = new ArrayList<Integer>();

  /**
   * Parse column mapping set by the user.
   * Since the mapping is different based on HashMap or ArrayList implement this in concrete derived class.
   * This is needed only for output operator.
   *
   * @param mapping
   */
  protected abstract void parseMapping(ArrayList<String> mapping);

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

  public void closeJDBCConnection()
  {
    try {
      connection.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException("Error while closing database resource", ex);
    }
  }
}
