/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.Context.OperatorContext;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCTransactionOutputOperator<T> extends JDBCOutputOperator<T>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCTransactionOutputOperator.class);
  protected Statement transactionStatement;

  public void initTransactionInfo(OperatorContext context)
  {
    try {
      transactionStatement = getConnection().createStatement();
      DatabaseMetaData meta = getConnection().getMetaData();
      ResultSet rs1 = meta.getTables(null, null, "maxwindowid", null);
      if (rs1.next() == false) {
//        logger.debug("table not exist!");
        String createSQL = "CREATE TABLE maxwindowid(appid varchar(32) not null, operatorid varchar(32) not null, winid bigint not null)";
        transactionStatement.execute(createSQL);
        String insertSQL = "INSERT maxwindowid set appid=0, winid=0, operatorid='" + context.getId() + "'";
        transactionStatement.executeUpdate(insertSQL);
      }

      String querySQL = "SELECT winid FROM maxwindowid LIMIT 1";
      ResultSet rs = transactionStatement.executeQuery(querySQL);
      if (rs.next() == false) {
        logger.error("max windowId table not ready!");
        return;
      }
      lastWindowId = rs.getLong("winid");
      getConnection().setAutoCommit(false);
      logger.debug("lastWindowId:" + lastWindowId);
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
    super.setup(context);
    initTransactionInfo(context);
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    if (windowId <= lastWindowId) {
      ignoreWindow = true;
    }

  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
    try {
      if (windowId > lastWindowId) {
        String str = "UPDATE maxwindowid set winid=" + windowId + " WHERE appid=0";
        transactionStatement.execute(str);
        getConnection().commit();
      }
    }
    catch (SQLException ex) {
      logger.debug(ex.toString());
    }
  }

  @Override
  public void processTuple(T tuple)
  {
  }
}
