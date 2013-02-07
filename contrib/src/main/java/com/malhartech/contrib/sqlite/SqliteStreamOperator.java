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
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
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

  public SqliteStreamOperator()
  {
    super();
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF);
  }

  public static class InputSchema
  {
    /**
     * the name of the input "table"
     */
    public String name;
    /**
     * key is the name of the column, and value is the SQL type
     */
    public HashMap<String, String> columnTypes = new HashMap<String, String>();

    private InputSchema()
    {
    }

    public InputSchema(String name)
    {
      this.name = name;
    }

    public void setColumnType(String columnName, String columnType)
    {
      columnTypes.put(columnName, columnType);
    }

  }

  protected String statement;
  protected InputSchema[] inputSchemas = new InputSchema[5];
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
    inputSchemas[inputPortIndex] = inputSchema;
  }

  @Override
  public void beginWindow(long windowId)
  {
    db = new SQLiteConnection(new File("/tmp/sqlite.db"));
    try {
      db.open(true);
      // create the temporary tables here
      for (InputSchema inputSchema: inputSchemas) {
        if (inputSchema == null || inputSchema.columnTypes.isEmpty()) {
          continue;
        }
        String columnSpec = "";
        for (Map.Entry<String, String> entry: inputSchema.columnTypes.entrySet()) {
          if (!columnSpec.isEmpty()) {
            columnSpec += ",";
          }
          columnSpec += entry.getKey();
          columnSpec += " ";
          columnSpec += entry.getValue();
        }
        String createTempTableStmt = "CREATE TEMP TABLE " + inputSchema.name + "(" + columnSpec + ")";
        SQLiteStatement st = db.prepare(createTempTableStmt);
        st.step();
        st.dispose();
      }
    }
    catch (SQLiteException ex) {
      Logger.getLogger(SqliteStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void processTuple(int tableNum, HashMap<String, Object> tuple)
  {
    InputSchema inputSchema = inputSchemas[tableNum];
    String columnNames = "";
    String columnValues = "";
    for (Map.Entry<String, Object> entry: tuple.entrySet()) {
      if (!columnNames.isEmpty()) {
        columnNames += ",";
        columnValues += ",";
      }
      columnNames += entry.getKey();
      columnValues += "'" + StringEscapeUtils.escapeSql(entry.getValue().toString()) + "'";
    }

    String insertStmt = "INSERT INTO " + inputSchema.name + " (" + columnNames + ") VALUES (" + columnValues + ")";
    try {
      SQLiteStatement st = db.prepare(insertStmt);
      st.step();
    }
    catch (SQLiteException ex) {
      Logger.getLogger(SqliteStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  @Override
  public void endWindow()
  {
    try {
      SQLiteStatement st = db.prepare(statement);
      if (bindings != null) {
        for (int i = 0; i < bindings.size(); i++) {
          st.bind(i, bindings.get(i).toString());
        }
      }
      while (st.step()) {
        int columnCount = st.columnCount();
        HashMap<String, Object> resultRow = new HashMap<String, Object>();
        for (int i = 0; i < columnCount; i++) {
          resultRow.put(st.getColumnName(i), st.columnValue(i));
        }
        this.result.emit(resultRow);
      }
    }
    catch (SQLiteException ex) {
      Logger.getLogger(SqliteStreamOperator.class.getName()).log(Level.SEVERE, null, ex);
    }
    db.dispose();
  }

}
