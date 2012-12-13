/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCArrayListOutputOperator extends JDBCTransactionOutputOperator<ArrayList<AbstractMap.SimpleEntry<String, Object>>>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCArrayListOutputOperator.class);

  @Override
  public void processTuple(ArrayList<AbstractMap.SimpleEntry<String, Object>> tuple)
  {
    try {
      int num = tuple.size();
      for (int idx = 0; idx < num; idx++) {
        String key = tuple.get(idx).getKey();
        getInsertStatement().setObject(
                getKeyToIndex().get(key).intValue(),
                tuple.get(idx).getValue(),
                getColumnSQLTypes().get(getKeyToType().get(key)));
      }
    }
    catch (SQLException ex) {
      logger.debug("exception while update", ex);
    }
  }
}
