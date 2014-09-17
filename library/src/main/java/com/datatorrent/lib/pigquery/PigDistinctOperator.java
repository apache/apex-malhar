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

import java.util.HashSet;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;


/**
 * Implements pig distinct operator semantic.
 *<p>
 * <pre>
 * Example
 *
 * Suppose we have relation A.
 *
 * A = LOAD 'data' AS (a1:int,a2:int,a3:int);
 *
 * DUMP A;
 * (8,3,4)
 * (1,2,3)
 * (4,3,3)
 * (4,3,3)
 * (1,2,3)
 *
 * In this example all duplicate tuples are removed.
 *
 * X = DISTINCT A;
 *
 * DUMP X;
 * (1,2,3)
 * (4,3,3)
 * (8,3,4)
 *
 * </pre>
 *
 * <b>StateFull : </b> Yes, tuples are collected over application window. <br>
 * <b>Partitions : </b> Yes, operator is also unifier for output port. <br>
 * <br>
 * This operator is pass thru operator. <br>
 * <br>
 * <b>Ports : </b> <br>
 * <b> inport : </b> expects tuple of form Map&lt;String, Object&gt; <br>
 * <b> outport : </b> emits tuple of form Map&lt;String, Object&gt; <br>
 * 
 * @displayName: Pig Distinct Operator
 * @category: pigquery
 * @tag: map, string, distinct
 * @since 0.3.4
 */
public class PigDistinctOperator implements Operator, Unifier<Map<String, Object>>
{
  /**
   * Distinct tuples set.
   */
  HashSet<Map<String, Object>>  distinctSet;
  
  /**
   * Input port 1.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      if (isDistinct(tuple)) {
        outport.emit(tuple);
        distinctSet.add(tuple);
      }
    } 
  };
  
  /**
   * Output port.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport = 
      new DefaultOutputPort<Map<String, Object>>()
  {
    @Override
    public PigDistinctOperator getUnifier()
    {
      return new PigDistinctOperator();
    }
  };
  
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
    distinctSet = new HashSet<Map<String, Object>>();
  }

  /* (non-Javadoc)
   * @see com.datatorrent.api.Operator#endWindow()
   */
  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see com.datatorrent.api.Operator.Unifier#process(java.lang.Object)
   */
  @Override
  public void process(Map<String, Object> tuple)
  {
    if (isDistinct(tuple)) {
      outport.emit(tuple);
      distinctSet.add(tuple);
    }
  }
  
  /**
   * Check for distinct tuple value. 
   */
  private boolean isDistinct(@NotNull Map<String, Object> tuple)
  {
    if (distinctSet.size() == 0) return true;
    for (Map<String, Object> compare : distinctSet) {
      if (isDistict(tuple, compare)) return true;
    }
    return false;
  }
  private boolean isDistict(@NotNull Map<String, Object> tuple,@NotNull Map<String, Object> compare)
  {
    for (Map.Entry<String, Object> entry : tuple.entrySet()) {
      if (!compare.containsKey(entry.getKey())) return false;
      if (!entry.getValue().equals(compare.get(entry.getKey()))) return false;
    }
    for (Map.Entry<String, Object> entry : compare.entrySet()) {
      if (!tuple.containsKey(entry.getKey())) return false;
      if (!entry.getValue().equals(tuple.get(entry.getKey()))) return false;
    }
    return true;
  }
}
