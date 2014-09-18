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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.jdbc.AbstractJdbcNonTransactionableOutputOperator;

/**
 * JDBCNonTransaction output adapter operator, which send insertion data to non-transaction database.
 * <p>
 * For non-transactional database, each row is committed as they are inserted into database.
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port which is derived from JDBCOutputOperator base class. <br>
 * <b>Output</b>: No output port. <br>
 * <br>
 * Properties:<br>
 * <b>statement</b>:The statement being used to save non-transaction database last windowId information in the tables. <br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * Non-transaction database requires additional columns operatorId, windowId, applicationId <br>
 * to store the last committed windowId information for recovery purpose.<br>
 * User needs to create the additional columns and assign the column names as windowIdColumnName,operatorIdColumnName,applicationIdColumnName.<br>
 * <br>
 * <b>Benchmarks</b>:
 * <br>
 * </p>
 *
 * @displayName JDBC Non Transaction Output Operator
 * @category db
 * @tags output
 *
 * @since 0.3.2
 * @deprecated use {@link AbstractJdbcNonTransactionableOutputOperator}
 */
@Deprecated
public abstract class JDBCNonTransactionOutputOperator<T> extends JDBCOutputOperator<T>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCNonTransactionOutputOperator.class);
  protected Statement statement;

  /**
   * Additional column name needed for non-transactional database recovery.
   * These column names should be present in addition to data columns that at coming from tuple.
   * Currently has windowId, operatorId, applicationId column information to be saved in the tables.
   * @return list of column names
   */
  @Override
  public ArrayList<String> windowColumn()
  {
    ArrayList<String> al = new ArrayList<String>();
    al.add(windowIdColumnName);
    al.add(operatorIdColumnName);
    al.add(applicationIdColumnName);
    return al;
  }

  /**
   * Initialize the last completed windowId as lastWindowId for the specific operatorId.
   * If the table is empty, the lastWindowId would be 0.
   */
  public void initLastWindowInfo()
  {
    int num = tableNames.size();
    for (int i = 0; i < num; ++i) {
      try {
        statement = getConnection().createStatement();
        String stmt = "SELECT MAX(" + windowIdColumnName + ") AS maxwinid FROM " + tableNames.get(0);
        ResultSet rs = statement.executeQuery(stmt);
        logger.debug(stmt);
        if (rs.next() == false) {
          logger.error("table " + tableNames.get(0) + " " + windowIdColumnName + " column not ready!");
          throw new RuntimeException("table " + tableNames.get(0) + " " + windowIdColumnName + " column not ready!");
        }
        lastWindowId = rs.getLong("maxwinid");
      }
      catch (SQLException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Implement Component Interface.
   * Initialize last finished window information during setup. Get the lastWindowId for the last completed windowId for the specific operatorId.
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
   * Compare windowId with lastWindowId, if it is less, it is out of date window, thus ignore it.
   * If it is the same as windowId, then it is the previously partially done window, then remove all the rows of this window, and reprocess all the tuples of this window
   * if it is greater than windowId, then process it.
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
        String stmt = "DELETE FROM " + getTableName() + " WHERE " + windowIdColumnName + "=" + windowId;
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
