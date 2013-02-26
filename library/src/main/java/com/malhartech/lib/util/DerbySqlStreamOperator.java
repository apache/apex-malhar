/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.lib.util;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.util.AbstractSqlStreamOperator.InputSchema.ColumnInfo;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class DerbySqlStreamOperator extends AbstractSqlStreamOperator
{
  protected transient ArrayList<PreparedStatement> insertStatements = new ArrayList<PreparedStatement>(5);
  protected transient PreparedStatement execStatement;
  protected transient ArrayList<PreparedStatement> deleteStatements = new ArrayList<PreparedStatement>(5);
  protected transient Connection db;

  @Override
  public void setup(OperatorContext context)
  {
    try {
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    }
    catch (Exception ex) {
      Logger.getLogger(DerbySqlStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
      return;
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
        for (Map.Entry<String, ColumnInfo> entry: inputSchema.columnInfoMap.entrySet()) {
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
        String createTempTableStmt = "DECLARE GLOBAL TEMPORARY TABLE SESSION." + inputSchema.name + "(" + columnSpec + ") NOT LOGGED";
        st = db.prepareStatement(createTempTableStmt);
        st.execute();
        st.close();

        String insertStmt = "INSERT INTO SESSION." + inputSchema.name + " (" + columnNames + ") VALUES (" + insertQuestionMarks + ")";

        insertStatements.add(i, db.prepareStatement(insertStmt));
        deleteStatements.add(i, db.prepareStatement("DELETE FROM SESSION." + inputSchema.name));
      }
      execStatement = db.prepareStatement(statement);
    }
    catch (SQLException ex) {
      Logger.getLogger(DerbySqlStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    try {
      db.setAutoCommit(false);
    }
    catch (SQLException ex) {
      Logger.getLogger(DerbySqlStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void processTuple(int tableNum, HashMap<String, Object> tuple)
  {
    InputSchema inputSchema = inputSchemas.get(tableNum);

    PreparedStatement insertStatement = insertStatements.get(tableNum);
    try {
      for (Map.Entry<String, Object> entry: tuple.entrySet()) {
        ColumnInfo t = inputSchema.columnInfoMap.get(entry.getKey());
        if (t != null && t.bindIndex != 0) {
          //System.out.println("Binding: "+entry.getValue().toString()+" to "+t.bindIndex);
          insertStatement.setString(t.bindIndex, entry.getValue().toString());
        }
      }

      insertStatement.executeUpdate();
      insertStatement.clearParameters();
    }
    catch (SQLException ex) {
      Logger.getLogger(DerbySqlStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void endWindow()
  {
    try {
      db.commit();
      if (bindings != null) {
        for (int i = 0; i < bindings.size(); i++) {
          execStatement.setString(i, bindings.get(i).toString());
        }
      }
      ResultSet res = execStatement.executeQuery();
      ResultSetMetaData resmeta = res.getMetaData();
      int columnCount = resmeta.getColumnCount();
      while (res.next()) {
        HashMap<String, Object> resultRow = new HashMap<String, Object>();
        for (int i = 1; i <= columnCount; i++) {
          resultRow.put(resmeta.getColumnName(i), res.getObject(i));
        }
        this.result.emit(resultRow);
      }
      execStatement.clearParameters();

      for (PreparedStatement st: deleteStatements) {
        st.executeUpdate();
        st.clearParameters();
      }
    }
    catch (SQLException ex) {
      Logger.getLogger(DerbySqlStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
    bindings = null;
  }

  @Override
  public void teardown()
  {
    try {
      db.close();
    }
    catch (SQLException ex) {
      Logger.getLogger(DerbySqlStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

}
