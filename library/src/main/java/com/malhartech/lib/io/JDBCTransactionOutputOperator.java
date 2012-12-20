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
public abstract class JDBCTransactionOutputOperator<T> extends JDBCOutputOperator<T>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCTransactionOutputOperator.class);
  protected Statement transactionStatement;
  private String operatorId;
  private String maxWindowTable;

  public String getMaxWindowTable()
  {
    return maxWindowTable;
  }

  public void setMaxWindowTable(String maxWindowTable)
  {
    this.maxWindowTable = maxWindowTable;
  }

  public void initTransactionInfo(OperatorContext context)
  {
    try {
      transactionStatement = getConnection().createStatement();
      DatabaseMetaData meta = getConnection().getMetaData();
      ResultSet rs1 = meta.getTables(null, null,maxWindowTable, null);
      if (rs1.next() == false) {
        logger.error(maxWindowTable+" table not exist!");
        throw new RuntimeException(maxWindowTable+" table not exist!");
      }

      String querySQL = "SELECT "+sWindowId+" FROM "+maxWindowTable+" WHERE "+sOperatorId+"='" + context.getId() + "' AND "+sApplicationId+"=" + 0; // how can I get the appid
      ResultSet rs = transactionStatement.executeQuery(querySQL);
      if (rs.next() == false) {
        String insertSQL = "INSERT "+maxWindowTable+" set "+sApplicationId+"=0, "+sWindowId+"=0, "+sOperatorId+"='" + context.getId() + "'";
        transactionStatement.executeUpdate(insertSQL);
        logger.debug(insertSQL);
        lastWindowId = 0;
      }
      else {
        lastWindowId = rs.getLong(sWindowId);
      }
      getConnection().setAutoCommit(false);
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
    initTransactionInfo(context);
    operatorId = context.getId();
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.windowId = windowId;
    if (windowId <= lastWindowId) {
      ignoreWindow = true;
    }
    else {
      ignoreWindow = false;
    }
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
    if (ignoreWindow) {
      return;
    }
    super.endWindow();
    try {
      String str = "UPDATE "+maxWindowTable+" set "+sWindowId+"=" + windowId + " WHERE "+sApplicationId+"=0 AND "+sOperatorId+"='" + operatorId + "'";
      transactionStatement.execute(str);
      logger.debug(str);
      getConnection().commit();
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }
}
