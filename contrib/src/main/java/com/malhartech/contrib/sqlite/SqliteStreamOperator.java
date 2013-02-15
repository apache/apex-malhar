/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.sqlite;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.contrib.sqlite.SqliteStreamOperator.InputSchema.ColumnInfo;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringEscapeUtils;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class SqliteStreamOperator extends BaseOperator
{
  public static class InputSchema
  {
    public static class ColumnInfo
    {
      public String type;
      public int bindIndex = 0;
      public boolean isColumnIndex = false;
    }

    /**
     * the name of the input "table"
     */
    public String name;
    /**
     * key is the name of the column, and value is the SQL type
     */
    public HashMap<String, ColumnInfo> columnInfoMap = new HashMap<String, ColumnInfo>();

    private InputSchema()
    {
    }

    public InputSchema(String name)
    {
      this.name = name;
    }

    public void setColumnInfo(String columnName, String columnType, boolean isColumnIndex)
    {
      ColumnInfo t = new ColumnInfo();
      t.type = columnType;
      t.isColumnIndex = isColumnIndex;
      columnInfoMap.put(columnName, t);
    }

  }

  protected String statement;
  protected transient ArrayList<SQLiteStatement> insertStatements = new ArrayList<SQLiteStatement>(5);
  protected transient SQLiteStatement beginStatement;
  protected transient SQLiteStatement commitStatement;
  protected transient SQLiteStatement execStatement;
  protected transient ArrayList<SQLiteStatement> deleteStatements = new ArrayList<SQLiteStatement>(5);
  protected ArrayList<InputSchema> inputSchemas = new ArrayList<InputSchema>(5);
  protected transient ArrayList<Object> bindings;
  protected transient SQLiteConnection db;
  @InputPortFieldAnnotation(name = "bindings", optional = true)
  public final transient DefaultInputPort<ArrayList<Object>> bindingsPort = new DefaultInputPort<ArrayList<Object>>(this)
  {
    @Override
    public void process(ArrayList<Object> tuple)
    {
      bindings = tuple;
    }

  };
  @InputPortFieldAnnotation(name = "in1")
  public final transient DefaultInputPort<HashMap<String, Object>> in1 = new DefaultInputPort<HashMap<String, Object>>(this)
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(0, tuple);
    }

  };
  @InputPortFieldAnnotation(name = "in2", optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in2 = new DefaultInputPort<HashMap<String, Object>>(this)
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(1, tuple);
    }

  };
  @InputPortFieldAnnotation(name = "in3", optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in3 = new DefaultInputPort<HashMap<String, Object>>(this)
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(2, tuple);
    }

  };
  @InputPortFieldAnnotation(name = "in4", optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in4 = new DefaultInputPort<HashMap<String, Object>>(this)
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(3, tuple);
    }

  };
  @InputPortFieldAnnotation(name = "in5", optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in5 = new DefaultInputPort<HashMap<String, Object>>(this)
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(4, tuple);
    }

  };
  @OutputPortFieldAnnotation(name = "result", optional = true)
  public final transient DefaultOutputPort<HashMap<String, Object>> result = new DefaultOutputPort<HashMap<String, Object>>(this);

  public void setStatement(String statement)
  {
    this.statement = statement;
  }

  public void setInputSchema(int inputPortIndex, InputSchema inputSchema)
  {
    inputSchemas.add(inputPortIndex, inputSchema);
  }

  @Override
  public void setup(OperatorContext context)
  {
    db = new SQLiteConnection(new File("/tmp/sqlite.db"));
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    SQLiteStatement st;

    try {
      db.open(true);
      // create the temporary tables here
      for (int i = 0; i < inputSchemas.size(); i++) {
        InputSchema inputSchema = inputSchemas.get(i);
        ArrayList<String> indexes = new ArrayList<String>();
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
          if (entry.getValue().isColumnIndex) {
            indexes.add(entry.getKey());
          }
          columnNames += entry.getKey();
          insertQuestionMarks += "?";
          entry.getValue().bindIndex = ++j;
        }
        String createTempTableStmt = "CREATE TEMP TABLE " + inputSchema.name + "(" + columnSpec + ")";
        st = db.prepare(createTempTableStmt);
        st.step();
        st.dispose();
        for (String index: indexes) {
          String createIndexStmt = "CREATE INDEX " + inputSchema.name + "_" + index + "_idx ON " + inputSchema.name + " (" + index + ")";
          st = db.prepare(createIndexStmt);
          st.step();
          st.dispose();
        }
        String insertStmt = "INSERT INTO " + inputSchema.name + " (" + columnNames + ") VALUES (" + insertQuestionMarks + ")";

        insertStatements.add(i, db.prepare(insertStmt));
        // We are calling "DELETE FROM" on the tables and because of the "truncate optimization" in sqlite, it should be fast.
        // See http://sqlite.org/lang_delete.html
        deleteStatements.add(i, db.prepare("DELETE FROM " + inputSchema.name));
      }
      beginStatement = db.prepare("BEGIN");
      commitStatement = db.prepare("COMMIT");
      execStatement = db.prepare(statement);
    }
    catch (SQLiteException ex) {
      Logger.getLogger(SqliteStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    try {
      beginStatement.step();
      beginStatement.reset();
    }
    catch (SQLiteException ex) {
      Logger.getLogger(SqliteStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void processTuple(int tableNum, HashMap<String, Object> tuple)
  {
    InputSchema inputSchema = inputSchemas.get(tableNum);

    SQLiteStatement insertStatement = insertStatements.get(tableNum);
    try {
      for (Map.Entry<String, Object> entry: tuple.entrySet()) {
        ColumnInfo t = inputSchema.columnInfoMap.get(entry.getKey());
        if (t != null && t.bindIndex != 0) {
          //System.out.println("Binding: "+entry.getValue().toString()+" to "+t.bindIndex);
          insertStatement.bind(t.bindIndex, entry.getValue().toString());
        }
      }

      insertStatement.step();
      insertStatement.reset();
    }
    catch (SQLiteException ex) {
      Logger.getLogger(SqliteStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void endWindow()
  {
    try {
      commitStatement.step();
      commitStatement.reset();
      if (bindings != null) {
        for (int i = 0; i < bindings.size(); i++) {
          execStatement.bind(i, bindings.get(i).toString());
        }
      }
      while (execStatement.step()) {
        int columnCount = execStatement.columnCount();
        HashMap<String, Object> resultRow = new HashMap<String, Object>();
        for (int i = 0; i < columnCount; i++) {
          resultRow.put(execStatement.getColumnName(i), execStatement.columnValue(i));
        }
        this.result.emit(resultRow);
      }
      execStatement.reset();

      for (SQLiteStatement st: deleteStatements) {
        st.step();
        st.reset();
      }
    }
    catch (SQLiteException ex) {
      Logger.getLogger(SqliteStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
    bindings = null;
  }

  @Override
  public void teardown()
  {
    db.dispose();
  }

}
