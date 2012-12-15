/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.sql.SQLException;
import java.sql.Types;
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
  public void processTuple(HashMap<String, V> tuple) throws SQLException
  {
    for (Map.Entry<String, V> e: tuple.entrySet()) {
      getInsertStatement().setObject(
              getKeyToIndex().get(e.getKey()).intValue(),
              e.getValue(),
              getSQLColumnType(getKeyToType().get(e.getKey())));
    }
    getInsertStatement().setObject(tuple.size() + 1, windowId, Types.BIGINT);
  }
}
