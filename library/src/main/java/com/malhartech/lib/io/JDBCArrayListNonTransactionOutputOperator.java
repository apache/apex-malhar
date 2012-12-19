/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCArrayListNonTransactionOutputOperator extends JDBCNonTransactionOutputOperator<ArrayList<Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCArrayListNonTransactionOutputOperator.class);

  @Override
  protected void parseMapping(ArrayList<String> mapping)
  {
    int num = mapping.size();
    for (int idx = 0; idx < num; ++idx) {
      String[] cols = mapping.get(idx).split(DELIMITER);
      if (cols.length != 2) {
        throw new RuntimeException("Incorrect column mapping for ArrayList type");
      }
      columnNames.add(cols[0]);
      keyToType.put(cols[0], cols[1].toUpperCase().contains("VARCHAR") ? "VARCHAR" : cols[1].toUpperCase());
    }
  }

  @Override
  public void processTuple(ArrayList<Object> tuple) throws SQLException
  {
    int num = tuple.size();
    for (int idx = 0; idx < num; idx++) {
      getInsertStatement().setObject(
              idx + 1,
              tuple.get(idx),
              getSQLColumnType(keyToType.get(getColumnNames().get(idx))));
    }
    getInsertStatement().setObject(num + 1, windowId, Types.BIGINT);
  }
}
