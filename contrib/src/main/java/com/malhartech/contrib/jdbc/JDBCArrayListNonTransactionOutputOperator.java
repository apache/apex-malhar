/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC output adapter operator, for ArrayList column mapping and non-transaction type database write.<p><br>
 * The tuple can contain an array of java object. <br>
 *
 * Ports:<br>
 * <b>Input</b>: This has a single input port that writes data into database.<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None <br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCArrayListNonTransactionOutputOperator extends JDBCNonTransactionOutputOperator<ArrayList<Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCArrayListNonTransactionOutputOperator.class);
  private static final int colIdx = 0;
  private static final int typeIdx = 1;

  /**
   * The column mapping will have following pattern: <br>
   * [Table.]Column:Type <br>
   * Followings are two examples: <br>
   * t1.col1:INTEGER,t3.col2:BIGINT,t3.col5:CHAR,t2.col4:DATE,t1.col7:DOUBLE,t2.col6:VARCHAR(10),t1.col3:DATE <br>
   * col1:INTEGER,col2:BIGINT,col5:CHAR,col4:DATE,col7:DOUBLE,col6:VARCHAR(10),col3:DATE <br>
   *
   * @param mapping
   */
  @Override
  protected void parseMapping(ArrayList<String> mapping)
  {
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

  /*
   * Bind tuple values into insert statements.
   * @param tuple
   */
  @Override
  public void processTuple(ArrayList<Object> tuple) throws SQLException
  {
    if (tuple.isEmpty()) {
      emptyTuple = true;
    }

    int num = tuple.size();
    for (int idx = 0; idx < num; idx++) {
      tableToInsertStatement.get(tableArray.get(idx)).setObject(
              columnIndexArray.get(idx),
              tuple.get(idx),
              getSQLColumnType(typeArray.get(idx)));
    }

    for (Map.Entry<String, PreparedStatement> entry: tableToInsertStatement.entrySet()) {
      entry.getValue().setObject(tableToColumns.get(entry.getKey()).size() + 1, windowId, Types.BIGINT);
      entry.getValue().setObject(tableToColumns.get(entry.getKey()).size() + 2, "0", Types.VARCHAR);
      entry.getValue().setObject(tableToColumns.get(entry.getKey()).size() + 3, operatorId, Types.VARCHAR);
    }
  }
}
