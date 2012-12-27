/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.mysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Key is string, Value can be any type derived from Java object.
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCHashMapOutputOperator<V> extends JDBCTransactionOutputOperator<HashMap<String, V>>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCHashMapOutputOperator.class);
  private static final int propIdx = 0;
  private static final int colIdx = 1;
  private static final int typeIdx = 2;

  @Override
  protected void parseMapping(ArrayList<String> mapping)
  {
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

  @Override
  public void processTuple(HashMap<String, V> tuple) throws SQLException
  {
    if (tuple.isEmpty()) {
      emptyTuple = true;
    }
    for (Map.Entry<String, V> e: tuple.entrySet()) {
      tableToInsertStatement.get(keyToTable.get(e.getKey())).setObject(
              keyToIndex.get(e.getKey()).intValue(),
              e.getValue(),
              getSQLColumnType(keyToType.get(e.getKey())));
    }
  }
}
