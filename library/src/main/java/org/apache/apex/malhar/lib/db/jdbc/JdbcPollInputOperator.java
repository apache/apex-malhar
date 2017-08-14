/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.db.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * A concrete implementation for {@link AbstractJdbcPollInputOperator} to
 * consume data from jdbc store and emit comma separated values <br>
 *
 * @displayName Jdbc Polling Input Operator
 * @category Input
 * @tags database, sql, jdbc
 *
 * @since 3.5.0
 */
@Evolving
public class JdbcPollInputOperator extends AbstractJdbcPollInputOperator<String>
{
  @OutputPortFieldAnnotation
  public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<>();
  private ArrayList<String> emitColumns;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    parseEmitColumnList();
  }

  private void parseEmitColumnList()
  {
    String[] cols = getColumnsExpression().split(",");
    emitColumns = Lists.newArrayList();
    for (int i = 0; i < cols.length; i++) {
      emitColumns.add(cols[i]);
    }
  }

  @Override
  public String getTuple(ResultSet rs)
  {
    StringBuilder resultTuple = new StringBuilder();
    try {
      int columnCount = rs.getMetaData().getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        resultTuple.append(rs.getObject(i + 1) + ",");
      }
      return resultTuple.substring(0, resultTuple.length() - 1); //remove last comma
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void emitTuple(String tuple)
  {
    outputPort.emit(tuple);
  }
}
