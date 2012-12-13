/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.sql.SQLException;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCArrayListOutputOperator extends JDBCTransactionOutputOperator<ArrayList<Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCArrayListOutputOperator.class);

  @Override
  public void processTuple(ArrayList<Object> tuple)
  {
    try {
      int num = tuple.size();
      if (num < 1) {
        emptyTuple = true;
      }
      for (int idx = 0; idx < num; idx++) {
        getInsertStatement().setObject(
                idx+1,
                tuple.get(idx),
                getColumnSQLTypes().get(getSimpleColumnToType().get(getColumnNames().get(idx))));
      }
    }
    catch (SQLException ex) {
      logger.debug("exception while update", ex);
    }
  }
}
