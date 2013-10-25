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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.lib.streamquery.function.FunctionIndex;

/**
 * <p>
 * This operator applies sql top/limit semantic on incoming tuple(s). <br>
 * <b>StateFull : Yes,</b> Operator aggregates input over application window. <br>
 * <b>Partitions : No, </b> will yield wrong result(s). <br>
 * <br>
 * <b>Ports : </b> <br>
 * <b>inport : </b> expect tuple for type T. <br>
 * <b>outport : </b> emits tuple for type T. <br>
 * <br>
 * <b> Properties : </b> <br>
 * <b> functions : </b> Sql function for rows. <br>
 *
 * @since 0.3.4
 */
@OperatorAnnotation(partitionable = false)
public class SelectFunctionOperator implements Operator
{
  /**
   * array of rows.
   */
  private ArrayList<Map<String, Object>> rows;
  
  /**
   * Aggregate function for rows.
   */
  private ArrayList<FunctionIndex> functions = new ArrayList<FunctionIndex>();

  /**
   * Input port.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport = new DefaultInputPort<Map<String, Object>>()
  {

    @Override
    public void process(Map<String, Object> row)
    {
      rows.add(row);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void beginWindow(long windowId)
  {
    rows = new ArrayList<Map<String, Object>>();
  }

  @Override
  public void endWindow()
  {
    if (functions.size() == 0) return;
    Map<String, Object>  collect = new HashMap<String, Object>();
    for (FunctionIndex function : functions) {
      try {
        function.filter(rows, collect);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
    }
    outport.emit(collect);
  }

  /**
   * Output port.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport = new DefaultOutputPort<Map<String, Object>>();

  /**
   * Add sql function.
   * @param function  Sql function for rows.
   */
  public void addSqlFunction(FunctionIndex function)
  {
    functions.add(function);
  }
}
