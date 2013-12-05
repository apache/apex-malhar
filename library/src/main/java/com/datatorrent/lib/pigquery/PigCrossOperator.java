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
package com.datatorrent.lib.pigquery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;


/**
 * This class implements Apache Pig Cross operator semantic.
 *
 * <pre>
 * Example
 * Suppose we have relations A and B.
 *
 * A = LOAD 'data1' AS (a1:int,a2:int,a3:int);
 *
 * DUMP A;
 * (1,2,3)
 * (4,2,1)
 *
 * B = LOAD 'data2' AS (b1:int,b2:int);
 *
 * DUMP B;
 * (2,4)
 * (8,9)
 * (1,3)
 * In this example the cross product of relation A and B is computed.
 *
 * X = CROSS A, B;
 *
 * DUMP X;
 * (1,2,3,2,4)
 * (1,2,3,8,9)
 * (1,2,3,1,3)
 * (4,2,1,2,4)
 * (4,2,1,8,9)
 * (4,2,1,1,3)
 * </pre>
 *  <br>
 * <b>StateFull : </b> Yes, tuples are collected over application window. <br>
 * <b>Partitions : </b> No, will yield wrong result. <br>
 * <br>
 * This operator is pass thru operator. <br>
 * <br>
 * <b>Ports : </b> <br>
 * <b> inport1 : </b> expects tuple of form Map&lt;String, Object&gt; <br>
 * <b> inport2 : </b> expects tuple of form Map&lt;String, Object&gt; <br>
 * <b> outport : </b> emits tuple of form Map&lt;String, Object&gt; <br>
 *
 * @since 0.3.4
 */
@OperatorAnnotation(partitionable = false)
public class PigCrossOperator implements Operator
{
  /**
   * Input port 1 tuples.
   */
  private ArrayList<Map<String, Object>> input1Tuples;
  
  /**
   * Input port 2 tuples.
   */
  private ArrayList<Map<String, Object>> input2Tuples;
  
  /**
   * Input port 1.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport1 = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      input1Tuples.add(tuple);
      for (Map<String, Object> record : input2Tuples) {
        emitCross(tuple, record);
      }
    }    
  };
 
  /**
   * Input port 2.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport2 = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      input2Tuples.add(tuple);
      for (Map<String, Object> record : input1Tuples) {
        emitCross(tuple, record);
      } 
    }
  };

  /**
   * Output port.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport = new DefaultOutputPort<Map<String, Object>>();
  
  /* (non-Javadoc)
   * @see com.datatorrent.api.Component#setup(com.datatorrent.api.Context)
   */
  @Override
  public void setup(OperatorContext context)
  {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see com.datatorrent.api.Component#teardown()
   */
  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see com.datatorrent.api.Operator#beginWindow(long)
   */
  @Override
  public void beginWindow(long windowId)
  {
    input1Tuples = new ArrayList<Map<String, Object>>();
    input2Tuples = new ArrayList<Map<String, Object>>();
  }

  /* (non-Javadoc)
   * @see com.datatorrent.api.Operator#endWindow()
   */
  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub
    
  }
  
  private void emitCross(Map<String, Object> tuple, Map<String, Object> record)
  {
    Map<String, Object> result = new HashMap<String, Object>(tuple);
    result.putAll(record);
    outport.emit(result);
  }
}
