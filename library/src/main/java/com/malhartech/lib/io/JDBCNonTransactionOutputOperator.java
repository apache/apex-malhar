/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCNonTransactionOutputOperator<V> extends JDBCOutputOperator<V>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCNonTransactionOutputOperator.class);
  private static int count = 0; // for debugging
  protected Statement statement;
  /**
   * The input port.
   */
  @InputPortFieldAnnotation(name = "inputPort")
  public final transient DefaultInputPort<HashMap<String, V>> inputPort = new DefaultInputPort<HashMap<String, V>>(this)
  {
    @Override
    public void process(HashMap<String, V> tuple)
    {
      if (windowId < lastWindowId) {
        return;
      }
      try {
        for (Map.Entry<String, V> e : tuple.entrySet()) {
          getInsertStatement().setString(getKeyToIndex().get(e.getKey()).intValue(), e.getValue().toString());
          count++;
        }
        getInsertStatement().setString(tuple.size() + 1, String.valueOf(windowId));
        getInsertStatement().executeUpdate();
      }
      catch (SQLException ex) {
        logger.debug("exception while update", ex);
      }

      logger.debug(String.format("count %d", count));
    }
  };

  public void initLastWindowInfo(String table)
  {
    try {
      statement = getConnection().createStatement();
      String stmt = "SELECT MAX(winid) AS winid FROM " + table;
      ResultSet rs = statement.executeQuery(stmt);
      if (rs.next() == false) {
        logger.error("table " + table + " winid column not ready!");
        return;
      }
      lastWindowId = rs.getLong("winid");
    }
    catch (SQLException ex) {
      logger.debug(ex.toString());
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
    setTransactionType("nonTransaction");
    buildMapping();
    setupJDBCConnection();
    prepareInsertStatement();
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    try {
      if (getInsertStatement() != null) {
        getInsertStatement().close();
      }
      getConnection().close();
    }
    catch (SQLException ex) {
      logger.debug("exception during teardown", ex);
    }
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    logger.debug("window:" + windowId);
    if (windowId == lastWindowId) {
      try {
        String stmt = "DELETE FROM " + getTableName() + " WHERE winid=" + windowId;
        statement.execute(stmt);
      }
      catch (SQLException ex) {
        logger.debug(ex.toString());
      }
    }
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
  }
}
