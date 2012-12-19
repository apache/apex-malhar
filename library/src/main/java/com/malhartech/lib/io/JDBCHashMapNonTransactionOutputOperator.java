/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCHashMapNonTransactionOutputOperator<V> extends JDBCNonTransactionOutputOperator<HashMap<String, V>>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCHashMapNonTransactionOutputOperator.class);

  @Override
  protected void parseMapping(ArrayList<String> mapping)
  {
    int num = mapping.size();
    for (int idx = 0; idx < num; ++idx) {
      String[] cols = mapping.get(idx).split(DELIMITER);
      if (cols.length < 2 || cols.length > 3) {
        throw new RuntimeException("Incorrect column mapping");
      }
      keyToIndex.put(cols[0], new Integer(idx + 1));
      columnNames.add(cols[1]);
      if (cols.length == 3) {
        keyToType.put(cols[0], cols[2].toUpperCase().contains("VARCHAR") ? "VARCHAR" : cols[2].toUpperCase());
      }
      else {
        keyToType.put(cols[0], "UNSPECIFIED");
      }
    }
  }


  @Override
  public void processTuple(HashMap<String, V> tuple) throws SQLException
  {
    for (Map.Entry<String, V> e: tuple.entrySet()) {
      getInsertStatement().setObject(
              keyToIndex.get(e.getKey()).intValue(),
              e.getValue(),
              getSQLColumnType(keyToType.get(e.getKey())));
    }
    getInsertStatement().setObject(tuple.size() + 1, windowId, Types.BIGINT);
  }
}
