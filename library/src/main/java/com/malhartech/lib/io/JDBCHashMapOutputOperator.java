/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.sql.SQLException;
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

  @Override
  public void processTuple(HashMap<String, V> tuple)
  {
    try {
      if (tuple.isEmpty()) {
        emptyTuple = true;
      }
      for (Map.Entry<String, V> e: tuple.entrySet()) {
        getInsertStatement().setObject(
                getKeyToIndex().get(e.getKey()).intValue(),
                e.getValue(),
                getColumnSQLTypes().get(getKeyToType().get(e.getKey())));
      }
    }
    catch (SQLException ex) {
      logger.debug("exception while update", ex);
    }
  }
}
