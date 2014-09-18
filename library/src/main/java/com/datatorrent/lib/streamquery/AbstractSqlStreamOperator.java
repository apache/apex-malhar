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
package com.datatorrent.lib.streamquery;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Abstract sql db input operator. 
 * <p>
 * @displayName: Abstract Sql Stream Operator
 * @category: streamquery
 * @tag: sql, hashmap, string
 * @since 0.3.2
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

    public InputSchema() {
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
  public final transient DefaultInputPort<ArrayList<Object>> bindingsPort = new DefaultInputPort<ArrayList<Object>>()
  {
    @Override
    public void process(ArrayList<Object> tuple)
    {
      bindings = tuple;
    }

  };
  @InputPortFieldAnnotation(name = "in1")
  public final transient DefaultInputPort<HashMap<String, Object>> in1 = new DefaultInputPort<HashMap<String, Object>>()
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(0, tuple);
    }

  };
  @InputPortFieldAnnotation(name = "in2", optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in2 = new DefaultInputPort<HashMap<String, Object>>()
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(1, tuple);
    }

  };
  @InputPortFieldAnnotation(name = "in3", optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in3 = new DefaultInputPort<HashMap<String, Object>>()
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(2, tuple);
    }

  };
  @InputPortFieldAnnotation(name = "in4", optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in4 = new DefaultInputPort<HashMap<String, Object>>()
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(3, tuple);
    }

  };
  @InputPortFieldAnnotation(name = "in5", optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in5 = new DefaultInputPort<HashMap<String, Object>>()
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(4, tuple);
    }

  };
  @OutputPortFieldAnnotation(name = "result", optional = true)
  public final transient DefaultOutputPort<HashMap<String, Object>> result = new DefaultOutputPort<HashMap<String, Object>>();

  public void setStatement(String statement)
  {
    this.statement = statement;
  }

  public String getStatement()
  {
    return this.statement;
  }

  public void setInputSchema(int inputPortIndex, InputSchema inputSchema)
  {
    inputSchemas.add(inputPortIndex, inputSchema);
  }

  public abstract void processTuple(int tableNum, HashMap<String, Object> tuple);

}
