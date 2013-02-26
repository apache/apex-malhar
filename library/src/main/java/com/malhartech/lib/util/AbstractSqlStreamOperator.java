/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.lib.util;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class AbstractSqlStreamOperator extends BaseOperator
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
  protected ArrayList<InputSchema> inputSchemas = new ArrayList<InputSchema>(5);
  protected transient ArrayList<Object> bindings;
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

  public abstract void processTuple(int tableNum, HashMap<String, Object> tuple);

}
