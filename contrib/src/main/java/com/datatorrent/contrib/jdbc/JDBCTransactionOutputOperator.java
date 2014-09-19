/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.jdbc;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A base implementation of an operator that writes data into a database using JAVA DataBase Connectivity (JDBC) API in
 * a transactional fashion.
 * <p>
 * For transaction output operator, user needs to have a separate table to store some metadata
 * which is needed to recover in case of a node failure. This additional table contain application id,
 * operator id, and max window id. User needs to have this table setup before running the operator
 * and set column names in the operator.
 * For transaction type operator, do commit into database only at the end of window boundary.
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port which is derived from JDBCOutputOperator base class. <br>
 * <b>Output</b>: No output port. <br>
 * <br>
 * Properties:<br>
 * <b> maxWindowTable</b>: The table name being used to save last windowId for the operator for recovery use.
 * <b>transactionStatement</b>: The statement being used to save transaction database max windowId information in maxWindowTable. <br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * maxWindowTable: For transaction database, needs to be set a name for the table. <br>
 * Transaction database requires creating a table  to store the last committed windowId, operatorId and applicationId for recovery purpose. <br>
 * User required to assign the name for the windowId, operatorId and applicationId column of the table.
 * <br>
 * <b>Benchmarks</b>:
 * </p>
 *
 * @displayName JDBC Transaction Output
 * @category database
 * @tags output operator
 *
 * @since 0.3.2
 * @deprecated use {@link AbstractJdbcTransactionableOutputOperator}
 */
@Deprecated
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
   * Initialize transaction database recovery information at setup. Read from maxWindowTable to get the last committed windowId.
   * If the maxWindowTable table is empty, insert a default value row for the operator with 0 value.
   * @param context
   */
  public void initTransactionInfo(OperatorContext context)
  {
    String selectSQL = null;
    String insertSQL = null;

    try {
      transactionStatement = getConnection().createStatement();
      DatabaseMetaData meta = getConnection().getMetaData();
      ResultSet rs1 = meta.getTables(null, null,maxWindowTable, null);
      if (rs1.next() == false) {
        logger.error(maxWindowTable+" table not exist!");
        throw new RuntimeException(maxWindowTable+" table not exist!");
      }

      selectSQL = "SELECT "+windowIdColumnName+" FROM "+maxWindowTable+" WHERE "+operatorIdColumnName+"=" + operatorId + " AND "+applicationIdColumnName+"='" + applicationId + "'";
      ResultSet rs = transactionStatement.executeQuery(selectSQL);
      if (rs.next() == false) {
        insertSQL = "INSERT "+maxWindowTable+" set "+applicationIdColumnName+ "='" +  applicationId + "', " +windowIdColumnName+"=0, "+operatorIdColumnName+"=" + operatorId;
        transactionStatement.executeUpdate(insertSQL);
        logger.debug(insertSQL);
        lastWindowId = 0;
      }
      else {
        lastWindowId = rs.getLong(windowIdColumnName);
      }
      // Don't do auto commit for transaction type operator.
      getConnection().setAutoCommit(false);
    }
    catch (SQLException ex) {
      throw new RuntimeException(String.format("Exception while setting maxwindowid table. select query: %s, insert query: %s", selectSQL, insertSQL), ex);
    }

  }

  /**
   * Implement Component Interface.
   * Initialize transaction database recovery information during setup.
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    initTransactionInfo(context);
  }

  /**
   * Start of a new window.
   * Check valid windowId: if windowId is less equal than lastWindowId, it is out of date window, thus ignore it.
   * If windowId is greater than lastWindowId, it is fresh window, thus process the window.
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
   * End of current window.
   * If it is out of date window, then ignore the window. If it is fresh window, update the current windowId in the maxWindowTable table,
   * and commit the transaction into database.
   */
  @Override
  public void endWindow()
  {
    if (ignoreWindow) {
      return;
    }
    super.endWindow();
    try {
      String str = "UPDATE "+maxWindowTable+" set "+windowIdColumnName+"=" + windowId + " WHERE "+applicationIdColumnName+"='" + applicationId + "' AND "+operatorIdColumnName+"=" + operatorId;
      transactionStatement.execute(str);
      logger.debug(str);
      getConnection().commit();
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }
}
