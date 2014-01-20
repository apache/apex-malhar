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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC output adapter operator, for ArrayList column mapping and non-transaction type database write.<p><br>
 * The tuple can contain an array of java object. <br>
 *
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
 *
 * @since 0.3.2
 */
public class JDBCNonTransactionArrayListOutputOperator extends JDBCNonTransactionOutputOperator<ArrayList<Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCNonTransactionArrayListOutputOperator.class);

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

    for (Map.Entry<String, PreparedStatement> entry: tableToInsertStatement.entrySet()) {
      entry.getValue().setObject(tableToColumns.get(entry.getKey()).size() + 1, windowId, Types.BIGINT);
      entry.getValue().setObject(tableToColumns.get(entry.getKey()).size() + 2, operatorId, Types.INTEGER);
      entry.getValue().setObject(tableToColumns.get(entry.getKey()).size() + 3, applicationId, Types.VARCHAR);
    }
  }
}
