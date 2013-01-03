/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.jdbc;

import com.malhartech.api.Context.OperatorContext;
import java.sql.*;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBCTransaction output adapter operator, which send insertion data to transaction database.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port which is derived from JDBCOutputOperator base class <br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b> maxWindowTable</b>: the table name being used to save last windowId for the operator for recovery use
 * <b>transactionStatement</b>:the statement being used to save transaction database max windowId information in maxWindowTable <br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * maxWindowTable: for transaction database,  needs to be set a name for the table<br>
 * transaction database requires creating a table  to store the last committed windowId, operatorId and applicationId for recovery purpose<br>
 * user required to assign the name for the windowId, operatorId and applicationId column of the table
 * <br>
 * <b>Benchmarks</b>:
 * <br>
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class JDBCTransactionOutputOperator<T> extends JDBCOutputOperator<T>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCTransactionOutputOperator.class);
  protected transient Statement transactionStatement;
  @NotNull
  private String maxWindowTable;

  public String getMaxWindowTable()
  {
    return maxWindowTable;
  }
  /**
   * set maxWindowTable table name
   * @param maxWindowTable
   */
  public void setMaxWindowTable(String maxWindowTable)
  {
    this.maxWindowTable = maxWindowTable;
  }

  /**
   * Init transaction database recovery information at setup. Read from maxWindowTable to get the last committed windowId.
   * If the maxWindowTable table is empty, insert a default value row for the operator with 0 value
   * @param context
   */
  public void initTransactionInfo(OperatorContext context)
  {
    String querySQL = null;
    String insertSQL = null;

    try {
      transactionStatement = getConnection().createStatement();
      DatabaseMetaData meta = getConnection().getMetaData();
      ResultSet rs1 = meta.getTables(null, null,maxWindowTable, null);
      if (rs1.next() == false) {
        logger.error(maxWindowTable+" table not exist!");
        throw new RuntimeException(maxWindowTable+" table not exist!");
      }

      querySQL = "SELECT "+windowIdColumnName+" FROM "+maxWindowTable+" WHERE "+operatorIdColumnName+"='" + context.getId() + "' AND "+applicationIdColumnName+"=" + 0; // how can I get the appid
      ResultSet rs = transactionStatement.executeQuery(querySQL);
      if (rs.next() == false) {
        insertSQL = "INSERT "+maxWindowTable+" set "+applicationIdColumnName+"=0, "+windowIdColumnName+"=0, "+operatorIdColumnName+"='" + context.getId() + "'";
        transactionStatement.executeUpdate(insertSQL);
        logger.debug(insertSQL);
        lastWindowId = 0;
      }
      else {
        lastWindowId = rs.getLong(windowIdColumnName);
      }
      getConnection().setAutoCommit(false);
    }
    catch (SQLException ex) {
      throw new RuntimeException(String.format("Exception while setting maxwindowid table. select query: %s, insert query: %s", querySQL, insertSQL), ex);
    }

  }

  /**
   * Implement Component Interface.
   * init transaction database recovery information during setup
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
   * check valid windowId: if windowId is less equal than lastWindowId, it is out of date window, thus ignore it.
   * If windowId is greater than lastWindowId, it is fresh window, thus process the window
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
   * If it is out of date window, then ignore the window. If it is fresh window, update the current windowId in the maxWindowTable table,
   * and commit the transaction into database
   */
  @Override
  public void endWindow()
  {
    if (ignoreWindow) {
      return;
    }
    super.endWindow();
    try {
      String str = "UPDATE "+maxWindowTable+" set "+windowIdColumnName+"=" + windowId + " WHERE "+applicationIdColumnName+"=0 AND "+operatorIdColumnName+"='" + operatorId + "'";
      transactionStatement.execute(str);
      logger.debug(str);
      getConnection().commit();
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }
}
