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

import java.util.ArrayList;
import java.util.HashMap;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * A base implementation of a BaseOperator that is a sql stream operator.&nbsp;  Subclasses should provide the
   implementation of how to process the tuples.
 * <p>
 * Abstract sql db input operator.
 * <p>
 * @displayName Abstract Sql Stream
 * @category Stream Manipulators
 * @tags sql operator
 * @since 0.3.2
 * @deprecated
 */
@Deprecated
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

    public InputSchema()
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

  /**
   * Input bindings port that takes an arraylist of objects.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<ArrayList<Object>> bindingsPort = new DefaultInputPort<ArrayList<Object>>()
  {
    @Override
    public void process(ArrayList<Object> tuple)
    {
      bindings = tuple;
    }

  };

  /**
   * Input port in1 that takes a hashmap of &lt;string,object&gt;.
   */
  public final transient DefaultInputPort<HashMap<String, Object>> in1 = new DefaultInputPort<HashMap<String, Object>>()
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(0, tuple);
    }

  };

  /**
   * Input port in2 that takes a hashmap of &lt;string,object&gt;.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in2 = new DefaultInputPort<HashMap<String, Object>>()
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(1, tuple);
    }

  };

  /**
   * Input port in3 that takes a hashmap of &lt;string,object&gt;.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in3 = new DefaultInputPort<HashMap<String, Object>>()
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(2, tuple);
    }

  };

  /**
   * Input port in4 that takes a hashmap of &lt;string,object&gt;.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in4 = new DefaultInputPort<HashMap<String, Object>>()
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(3, tuple);
    }

  };

  /**
   * Input port in5 that takes a hashmap of &lt;string,object&gt;.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<HashMap<String, Object>> in5 = new DefaultInputPort<HashMap<String, Object>>()
  {
    @Override
    public void process(HashMap<String, Object> tuple)
    {
      processTuple(4, tuple);
    }

  };

  /**
   * Output result port that emits a hashmap of &lt;string,object&gt;.
   */
  @OutputPortFieldAnnotation(optional = true)
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
