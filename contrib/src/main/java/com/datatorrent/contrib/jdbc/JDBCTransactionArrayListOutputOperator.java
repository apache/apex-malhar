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

import com.datatorrent.lib.db.jdbc.AbstractJdbcTransactionableOutputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;

/**
 * A base implementation of an operator that writes data into a database using JAVA DataBase Connectivity (JDBC) API in
 * a transactional fashion where the input is an array list of java objects.&nbsp;The table and column mappings are
 * specified in a list.
 * <p>
 * Each tuple contains an array of java object. <br>
 * <br>
 * Ports:<br>
 * <b>Input</b>: This has a single input port that writes data into database.<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None <br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * </p>
 *
 * @displayName JDBC Transaction Array List Output Operator
 * @category database
 * @tags output operator
 *
 * @since 0.3.2
 * @deprecated use {@link AbstractJdbcTransactionableOutputOperator}
 */
@Deprecated
public class JDBCTransactionArrayListOutputOperator extends JDBCTransactionOutputOperator<ArrayList<Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCTransactionArrayListOutputOperator.class);

  /**
   * @param mapping
   */
  @Override
  protected void parseMapping(ArrayList<String> mapping)
  {
    parseArrayListColumnMapping(mapping);
  }

  /*
   * Bind tuple values into insert statements.
   * @param tuple
   */
  @Override
  public void processTuple(ArrayList<Object> tuple) throws SQLException
  {
    if (tuple.isEmpty()) {
      emptyTuple = true;
    }
    int num = tuple.size();

    for (int idx = 0; idx < num; idx++) {
      tableToInsertStatement.get(tableArray.get(idx)).setObject(
              columnIndexArray.get(idx),
              tuple.get(idx),
              getSQLColumnType(typeArray.get(idx)));
    }
  }
}
