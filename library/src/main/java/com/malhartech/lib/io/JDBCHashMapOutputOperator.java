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
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCHashMapOutputOperator<V> extends JDBCTransactionOutputOperator<HashMap<String, V>>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCHashMapOutputOperator.class);
  private int count = 0;

  @Override
  public void processTuple(HashMap<String, V> tuple)
  {
    try {
      for (Map.Entry<String, V> e : tuple.entrySet()) {
        getInsertStatement().setString(getKeyToIndex().get(e.getKey()).intValue(), e.getValue().toString());
        count++;
      }
      getInsertStatement().executeUpdate();
    }
    catch (SQLException ex) {
      logger.debug("exception while update", ex);
    }

    logger.debug(String.format("count %d", count));
  }
}
