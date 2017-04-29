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
package org.apache.apex.malhar.contrib.misc.streamquery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.contrib.misc.streamquery.AbstractSqlStreamOperator.InputSchema.ColumnInfo;

import com.datatorrent.api.Context.OperatorContext;

/**
 * An implementation of AbstractSqlStreamOperator that provides embedded derby sql input operator.
 * <p>
 * @displayName Derby Sql Stream
 * @category Stream Manipulators
 * @tags sql, in-memory, input operator
 * @since 0.3.2
 * @deprecated
 */
@Deprecated
public class DerbySqlStreamOperator extends AbstractSqlStreamOperator
{
  protected transient ArrayList<PreparedStatement> insertStatements = new ArrayList<PreparedStatement>(5);
  protected List<String> execStmtStringList = new ArrayList<String>();
  protected transient ArrayList<PreparedStatement> execStatements = new ArrayList<PreparedStatement>(5);
  protected transient ArrayList<PreparedStatement> deleteStatements = new ArrayList<PreparedStatement>(5);
  protected transient Connection db;

  public void addExecStatementString(String stmt)
  {
    this.execStmtStringList.add(stmt);
  }


  @Override
  public void setup(OperatorContext context)
  {
    System.setProperty("derby.stream.error.file", "/dev/null");
    try {
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    String connUrl = "jdbc:derby:memory:MALHAR_TEMP;create=true";
    PreparedStatement st;

    try {
      db = DriverManager.getConnection(connUrl);
      // create the temporary tables here
      for (int i = 0; i < inputSchemas.size(); i++) {
        InputSchema inputSchema = inputSchemas.get(i);
        if (inputSchema == null || inputSchema.columnInfoMap.isEmpty()) {
          continue;
        }
        String columnSpec = "";
        String columnNames = "";
        String insertQuestionMarks = "";
        int j = 0;
        for (Map.Entry<String, ColumnInfo> entry : inputSchema.columnInfoMap.entrySet()) {
          if (!columnSpec.isEmpty()) {
            columnSpec += ",";
            columnNames += ",";
            insertQuestionMarks += ",";
          }
          columnSpec += entry.getKey();
          columnSpec += " ";
          columnSpec += entry.getValue().type;
          columnNames += entry.getKey();
          insertQuestionMarks += "?";
          entry.getValue().bindIndex = ++j;
        }
        String createTempTableStmt =
            "DECLARE GLOBAL TEMPORARY TABLE SESSION." + inputSchema.name + "(" + columnSpec + ") NOT LOGGED";
        st = db.prepareStatement(createTempTableStmt);
        st.execute();
        st.close();

        String insertStmt = "INSERT INTO SESSION." + inputSchema.name + " (" + columnNames + ") VALUES ("
            + insertQuestionMarks + ")";

        insertStatements.add(i, db.prepareStatement(insertStmt));
        deleteStatements.add(i, db.prepareStatement("DELETE FROM SESSION." + inputSchema.name));
      }
      for (String stmtStr : execStmtStringList) {
        execStatements.add(db.prepareStatement(stmtStr));
      }
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    try {
      db.setAutoCommit(false);
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void processTuple(int tableNum, HashMap<String, Object> tuple)
  {
    InputSchema inputSchema = inputSchemas.get(tableNum);

    PreparedStatement insertStatement = insertStatements.get(tableNum);
    try {
      for (Map.Entry<String, Object> entry : tuple.entrySet()) {
        ColumnInfo t = inputSchema.columnInfoMap.get(entry.getKey());
        if (t != null && t.bindIndex != 0) {
          insertStatement.setString(t.bindIndex, entry.getValue().toString());
        }
      }

      insertStatement.executeUpdate();
      insertStatement.clearParameters();
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void endWindow()
  {
    try {
      db.commit();
      if (bindings != null) {
        for (int i = 0; i < bindings.size(); i++) {
          for (PreparedStatement stmt : execStatements) {
            stmt.setString(i, bindings.get(i).toString());
          }
        }
      }

      for (PreparedStatement stmt : execStatements) {
        executePreparedStatement(stmt);
      }
      for (PreparedStatement st : deleteStatements) {
        st.executeUpdate();
        st.clearParameters();
      }
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
    bindings = null;
  }

  private void executePreparedStatement(PreparedStatement statement) throws SQLException
  {
    ResultSet res = statement.executeQuery();
    ResultSetMetaData resmeta = res.getMetaData();
    int columnCount = resmeta.getColumnCount();
    while (res.next()) {
      HashMap<String, Object> resultRow = new HashMap<String, Object>();
      for (int i = 1; i <= columnCount; i++) {
        resultRow.put(resmeta.getColumnName(i), res.getObject(i));
      }
      this.result.emit(resultRow);
    }
    statement.clearParameters();
  }

  @Override
  public void teardown()
  {
    try {
      db.close();
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

}
