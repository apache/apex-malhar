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

import com.datatorrent.api.Attribute.AttributeMap;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.DAG;


/**
 *
 */
public class JDBCOperatorTestHelper
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCOperatorTestHelper.class);
  protected static final String FIELD_DELIMITER = ":";
  protected static final String COLUMN_DELIMITER = ".";
  private Connection con = null;
  private Statement stmt = null;
  private static final String driver = "com.mysql.jdbc.Driver";
  private static final String url = "jdbc:mysql://localhost/test?user=test&password=";
  private static final String db_name = "test";
  private final HashMap<String, ArrayList<String>> tableToColumns2 = new HashMap<String, ArrayList<String>>(); // same as tableToColumn in operator
  protected transient HashMap<String, PreparedStatement> tableToInsertStatement = new HashMap<String, PreparedStatement>();
  protected transient HashMap<String, Integer> keyToIndex = new HashMap<String, Integer>();
  protected transient HashMap<String, String> keyToType = new HashMap<String, String>();
  protected transient HashMap<String, String> keyToTable = new HashMap<String, String>();
  protected transient ArrayList<String> columnNames = new ArrayList<String>();
  protected transient ArrayList<String> tableNames = new ArrayList<String>();
  private transient HashMap<String, Integer> columnSQLTypes = new HashMap<String, Integer>();
  private final int columnCount = 7;
  public AttributeMap attrmap = new DefaultAttributeMap();

  public String[] hashMapping1 = new String[7];
  public String[] hashMapping2 = new String[7];
  public String[] hashMapping3 = new String[7];
  public String[] arrayMapping1 = new String[7];
  public String[] arrayMapping2 = new String[7];
  public String[] arrayMapping3 = new String[7];
  public String[] hashMapping4 = new String[7];
  public String[] hashMapping5 = new String[7];
  public String[] arrayMapping4 = new String[7];
  public String[] arrayMapping5 = new String[7];

  public void buildDataset()
  {
    // mapping1
    hashMapping1[0] = "prop1:col1:INTEGER";
    hashMapping1[1] = "prop2:col2:INTEGER";
    hashMapping1[2] = "prop5:col5:INTEGER";
    hashMapping1[3] = "prop6:col4:INTEGER";
    hashMapping1[4] = "prop7:col7:INTEGER";
    hashMapping1[5] = "prop3:col6:INTEGER";
    hashMapping1[6] = "prop4:col3:INTEGER";

    // mapping2
    hashMapping2[0] = "prop1:col1:INTEGER";
    hashMapping2[1] = "prop2:col2:VARCHAR(10)";
    hashMapping2[2] = "prop5:col5:INTEGER";
    hashMapping2[3] = "prop6:col4:varchar(10)";
    hashMapping2[4] = "prop7:col7:integer";
    hashMapping2[5] = "prop3:col6:VARCHAR(10)";
    hashMapping2[6] = "prop4:col3:int";

    // mapping3
    hashMapping3[0] = "prop1:col1:INTEGER";
    hashMapping3[1] = "prop2:col2:BIGINT";
    hashMapping3[2] = "prop5:col5:CHAR";
    hashMapping3[3] = "prop6:col4:DATE";
    hashMapping3[4] = "prop7:col7:DOUBLE";
    hashMapping3[5] = "prop3:col6:VARCHAR(10)";
    hashMapping3[6] = "prop4:col3:DATE";

    // mapping 4
    arrayMapping1[0] = "col1:INTEGER";
    arrayMapping1[1] = "col2:BIGINT";
    arrayMapping1[2] = "col5:CHAR";
    arrayMapping1[3] = "col4:DATE";
    arrayMapping1[4] = "col7:double";
    arrayMapping1[5] = "col6:VARCHAR(10)";
    arrayMapping1[6] = "col3:DATE";

    // mapping 5
    arrayMapping2[0] = "col1:integer";
    arrayMapping2[1] = "col2:BAD_COLUMN_TYPE";
    arrayMapping2[2] = "col5:char";
    arrayMapping2[3] = "col4:DATE";
    arrayMapping2[4] = "col7:DOUBLE";
    arrayMapping2[5] = "col6:VARCHAR(10)";
    arrayMapping2[6] = "col3:date";

    // mapping 6
    arrayMapping3[0] = "col1:INTEGER";
    arrayMapping3[1] = "col2:BIGINT";
    arrayMapping3[2] = "col5:char";
    arrayMapping3[3] = "col4:DATE";
    arrayMapping3[4] = "col7:DOUBLE";
    arrayMapping3[5] = "col6:VARCHAR(10)";
    arrayMapping3[6] = "col3:date";

    // Single table mapping
    hashMapping4[0] = "prop1:t1.col1:INTEGER";
    hashMapping4[1] = "prop2:t1.col2:BIGINT";
    hashMapping4[2] = "prop5:t1.col5:CHAR";
    hashMapping4[3] = "prop6:t1.col4:DATE";
    hashMapping4[4] = "prop7:t1.col7:DOUBLE";
    hashMapping4[5] = "prop3:t1.col6:VARCHAR(10)";
    hashMapping4[6] = "prop4:t1.col3:DATE";

    // Multi table mapping
    hashMapping5[0] = "prop1:t1.col1:INTEGER";
    hashMapping5[1] = "prop2:t3.col2:BIGINT";
    hashMapping5[2] = "prop5:t3.col5:CHAR";
    hashMapping5[3] = "prop6:t2.col4:DATE";
    hashMapping5[4] = "prop7:t1.col7:DOUBLE";
    hashMapping5[5] = "prop3:t2.col6:VARCHAR(10)";
    hashMapping5[6] = "prop4:t1.col3:DATE";

    // Single table mapping
    arrayMapping4[0] = "t1.col1:INTEGER";
    arrayMapping4[1] = "t1.col2:BIGINT";
    arrayMapping4[2] = "t1.col5:CHAR";
    arrayMapping4[3] = "t1.col4:DATE";
    arrayMapping4[4] = "t1.col7:DOUBLE";
    arrayMapping4[5] = "t1.col6:VARCHAR(10)";
    arrayMapping4[6] = "t1.col3:DATE";

    // Multi table mapping
    arrayMapping5[0] = "t1.col1:INTEGER";
    arrayMapping5[1] = "t3.col2:BIGINT";
    arrayMapping5[2] = "t3.col5:CHAR";
    arrayMapping5[3] = "t2.col4:DATE";
    arrayMapping5[4] = "t1.col7:DOUBLE";
    arrayMapping5[5] = "t2.col6:VARCHAR(10)";
    arrayMapping5[6] = "t1.col3:DATE";

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

    attrmap.put(DAG.APPLICATION_ID, "myJDBCOutputOperatorAppId");

  }

  public HashMap<String, Object> hashMapData(String[] mapping, int i)
  {
    HashMap<String, Object> hm = new HashMap<String, Object>();
    for (int j = 1; j <= columnCount; ++j) {
      String[] parts = mapping[j - 1].split(":");
      if (parts[2].toUpperCase().contains("VARCHAR")) {
        parts[2] = "VARCHAR";
      }
      if ("INTEGER".equalsIgnoreCase(parts[2])
              || "INT".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], new Integer((columnCount * i) + j));
      }
      else if ("BIGINT".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], new Integer((columnCount * i) + j));
      }
      else if ("CHAR".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], 'a');
      }
      else if ("DATE".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], new Date());
      }
      else if ("DOUBLE".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], new Double((columnCount * i + j) / 3.0));
      }
      else if ("VARCHAR".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], "Test");
      }
      else if ("TIME".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], new Date());
      }
      else {
        throw new RuntimeException("Exception while generating data for hashMap");
      }
    }

    return hm;
  }

  public ArrayList<Object> arrayListData(String[] mapping, int i)
  {
    ArrayList<Object> al = new ArrayList<Object>();
    for (int j = 1; j <= columnCount; ++j) {
      String[] parts = mapping[j - 1].split(":");
      if (parts[1].toUpperCase().contains("VARCHAR")) {
        parts[1] = "VARCHAR";
      }
      if ("INTEGER".equalsIgnoreCase(parts[1])) {
        al.add(new Integer((columnCount * i) + j));
      }
      else if ("BIGINT".equalsIgnoreCase(parts[1])) {
        al.add(new Integer((columnCount * i) + j));
      }
      else if ("CHAR".equalsIgnoreCase(parts[1])) {
        al.add('a');
      }
      else if ("DATE".equalsIgnoreCase(parts[1])) {
        al.add(new Date());
      }
      else if ("DOUBLE".equalsIgnoreCase(parts[1])) {
        al.add(new Double((columnCount * i + j) / 3.0));
      }
      else if ("VARCHAR".equalsIgnoreCase(parts[1])) {
        al.add("Test");
      }
      else if ("TIME".equalsIgnoreCase(parts[1])) {
        al.add(new Date());
      }
      else if ("BAD_COLUMN_TYPE".equalsIgnoreCase(parts[1])) {
        al.add(new Integer((columnCount * i) + j));
      }
      else {
        throw new RuntimeException("Exception while generating data for arrayList");
      }
    }

    return al;
  }

  /*
   * Create database connection.
   * Create database if not exist.
   * For Transaction db, create data table as well as maxwindowid table.
   * For non-transation db, create only data table with additional columns application id, operator id, window id.
   */
  public void setupDB(JDBCOutputOperator oper, String[] mapping, boolean isHashMap, boolean transaction)
  {
    int num = mapping.length;
    int propIdx = isHashMap ? 0 : -1;
    int colIdx = isHashMap ? 1 : 0;
    int typeIdx = isHashMap ? 2 : 1;
    HashMap<String, String> columnToType = new HashMap<String, String>();
    tableToColumns2.clear();

    String table;
    String column;

    for (int idx = 0; idx < num; ++idx) {
      String[] fields = mapping[idx].split(":");
      if (fields.length < 2 || fields.length > 3) {
        throw new RuntimeException("Incorrect column mapping for HashMap. Correct mapping should be Property:\"[Table.]Column:Type\"");
      }

      int colDelIdx = fields[colIdx].indexOf(".");
      if (colDelIdx != -1) { // table name is used
        table = fields[colIdx].substring(0, colDelIdx);
        column = fields[colIdx].substring(colDelIdx + 1);
      }
      else { // table name not used; so this must be single table
        table = oper.getTableName();
        if (table.isEmpty()) {
          throw new RuntimeException("Table name can not be empty");
        }
        column = fields[colIdx];
      }

      if (tableToColumns2.containsKey(table)) {
        tableToColumns2.get(table).add(column);
      }
      else {
        ArrayList<String> cols = new ArrayList<String>();
        cols.add(column);
        tableToColumns2.put(table, cols);
      }
      columnToType.put(column, fields[typeIdx]);

      if (isHashMap) {
        keyToTable.put(fields[propIdx], table);
        keyToIndex.put(fields[propIdx], tableToColumns2.get(table).size());
        keyToType.put(fields[propIdx], fields[typeIdx].toUpperCase().contains("VARCHAR") ? "VARCHAR" : fields[typeIdx].toUpperCase());
      }
    }

    HashMap<String, String> tableToCreate = new HashMap<String, String>();
    for (Map.Entry<String, ArrayList<String>> entry: tableToColumns2.entrySet()) {
      String str = "";
      ArrayList<String> parts = entry.getValue();
      for (int i = 0; i < parts.size(); i++) {
        if (i == 0) {
          str += parts.get(i) + " " + columnToType.get(parts.get(i));
        }
        else {
          if (columnToType.get(parts.get(i)).equals("BAD_COLUMN_TYPE")) {
            str += ", " + parts.get(i) + " INTEGER";
          }
          else {
            str += ", " + parts.get(i) + " " + columnToType.get(parts.get(i));
          }
        }
      }

      if (!transaction) {
        str += ", " + oper.getApplicationIdColumnName() + " VARCHAR(32), "
                    + oper.getOperatorIdColumnName() + " INTEGER, "
                    + oper.getWindowIdColumnName() + " BIGINT";
      }

      String createTable = "CREATE TABLE " + entry.getKey() + " (" + str + ")";
      tableToCreate.put(entry.getKey(), createTable);
      logger.debug(createTable);
    }

    try {
      // This will load the JDBC driver, each DB has its own driver
      Class.forName(driver).newInstance();

      con = DriverManager.getConnection(url);
      stmt = con.createStatement();

      String createDB = "CREATE DATABASE IF NOT EXISTS " + db_name;
      String useDB = "USE " + db_name;

      stmt.executeUpdate(createDB);
      stmt.executeQuery(useDB);

      for (Map.Entry<String, String> entry: tableToCreate.entrySet()) {
        stmt.execute("DROP TABLE IF EXISTS " + entry.getKey()); // drop data table
        stmt.executeUpdate(entry.getValue());                   // create new data table
      }

      if (transaction) { // create maxwindowid table only first time
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS maxwindowid ("
                + oper.getApplicationIdColumnName() + " VARCHAR(32), "
                + oper.getOperatorIdColumnName() + " INTEGER, "
                + oper.getWindowIdColumnName() + " BIGINT)");
      }
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException("Exception during JBDC connection", ex);
    }
    catch (SQLException ex) {
      throw new RuntimeException(String.format("Exception during setupDB"), ex);
    }
    catch (Exception ex) {
      throw new RuntimeException("Exception during JBDC connection", ex);
    }

    logger.debug("JDBC Table creation Success");
  }

  /*
   * Read tuple from database after running the operator.
   */
  public void readDB(String tableName, String[] mapping, boolean isHashMap)
  {
    for (Map.Entry<String, ArrayList<String>> entry: tableToColumns2.entrySet()) {
      int num = entry.getValue().size();
      String query = "SELECT * FROM " + entry.getKey();
      try {
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
          String str = "";
          for (int i = 0; i < num; i++) {
            str += rs.getObject(i + 1).toString() + " ";
          }
          logger.debug(str);
        }
      }
      catch (SQLException ex) {
        throw new RuntimeException(String.format("Exception during reading from table %s", entry.getKey()), ex);
      }
    }
  }

  /*
   * Close database resources.
   */
  public void cleanupDB()
  {
    try {
      stmt.close();
      con.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException("Exception while closing database resource", ex);
    }
  }
}
