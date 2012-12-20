/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.Context.OperatorContext;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class JDBCNonTransactionOutputOperator<T> extends JDBCOutputOperator<T>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCNonTransactionOutputOperator.class);
  protected Statement statement;

  /**
   * Additional column name needed for non-transactional database.
   * @return
   */
  @Override
  public HashMap<String, String> windowColumn()
  {
    HashMap<String, String> hm = new HashMap<String, String>();
    hm.put(sOperatorId, "?");
    hm.put(sApplicationId, "?");
    hm.put(sWindowId, "?");
    return hm;
  }

  public void initLastWindowInfo()
  {
    try {
      statement = getConnection().createStatement();
      String stmt = "SELECT MAX(winid) AS winid FROM " + tableNames.get(0); // Need to work on multi table
      ResultSet rs = statement.executeQuery(stmt);
        logger.debug(stmt);
      if (rs.next() == false) {
        logger.error("table " + tableNames.get(0) + " winid column not ready!");
        throw new RuntimeException("table " + tableNames.get(0) + " winid column not ready!");
      }
      lastWindowId = rs.getLong("maxwinid");
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    initLastWindowInfo();
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    if (windowId < lastWindowId) {
      ignoreWindow = true;
    }
    else if (windowId == lastWindowId) {
      ignoreWindow = false;
      try {
        String stmt = "DELETE FROM " + getTableName() + " WHERE "+sWindowId+"=" + windowId;
        statement.execute(stmt);
        logger.debug(stmt);
      }
      catch (SQLException ex) {
        throw new RuntimeException("Error while deleting windowId from db", ex);
      }
    }
    else {
      ignoreWindow = false;
    }
  }
}
