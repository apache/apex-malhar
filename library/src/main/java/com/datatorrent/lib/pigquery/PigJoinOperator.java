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

import javax.validation.constraints.NotNull;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.lib.streamquery.condition.Condition;

/**
 * This class implements Pig Join(Inner) semantic on live stream.
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
 * (8,3,4)
 * (4,3,3)
 * (7,2,5)
 * (8,4,3)
 *
 * B = LOAD 'data2' AS (b1:int,b2:int);
 *
 * DUMP B;
 * (2,4)
 * (8,9)
 * (1,3)
 * (2,7)
 * (2,9)
 * (4,6)
 * (4,9)
 * In this example relations A and B are joined by their first fields.
 *
 * X = JOIN A BY a1, B BY b1;
 *
 * DUMP X;
 * (1,2,3,1,3)
 * (4,2,1,4,6)
 * (4,3,3,4,6)
 * (4,2,1,4,9)
 * (4,3,3,4,9)
 * (8,3,4,8,9)
 * (8,4,3,8,9)
 * </pre>
 * <br>
 * <b>Ports : </b> <br>
 * <b>inport1 : </b> expects tuple Map<String, Object>. <br>
 * <b>inport2 : </b> expects tuple Map<String, Object>. <br>
 * <b>outport : </b> emits joinde tuple Map<String, Object>. <br>
 * <br>
 * <b> StateFull : </b> Yes, values are aggregated over application window.  <br>
 * <b> Partitions : </b> No, will yield worng results. <br>
 * Operator is pass thru, output tuples are emitted in current time window.  <br>
 * <br>
 * <b>Properties : </b> <br>
 * <b> joinCondition : </b> Tuple join condition.
 *
 * @since 0.3.4
 */
@OperatorAnnotation(partitionable = false)
public class PigJoinOperator extends BaseOperator
{
    /**
     * Tuple join condition.
     */
    @NotNull
    private Condition joinCondition = null;
    
    /**
     * Aggregated tuples on inport 1.
     */
    private ArrayList<Map<String, Object>>  tuples1;
    
    /**
     * Aggregated tuples on inport 2.
     */
    private ArrayList<Map<String, Object>>  tuples2;
    
    /**
     * @param joinCondition   Join condition, must be non-null. 
     */
    public PigJoinOperator(@NotNull Condition joinCondition) {
      this.joinCondition = joinCondition;
    }
    
    /**
     * Input1 port.
     */
    public final transient DefaultInputPort<Map<String, Object>> inport1 = new DefaultInputPort<Map<String, Object>>()
    {
      @Override
      public void process(Map<String, Object> tuple)
      {
        tuples1.add(tuple);
        for (Map<String, Object> tuple2 : tuples2) {
          joinColumn(tuple, tuple2);
        }
      }
    };
    
    /**
     * Input2 port.
     */
    public final transient DefaultInputPort<Map<String, Object>> inport2 = new DefaultInputPort<Map<String, Object>>()
    {
      @Override
      public void process(Map<String, Object> tuple)
      {
        tuples2.add(tuple);
        for (Map<String, Object> tuple1 : tuples1) {
          joinColumn(tuple1, tuple);
        }
      }
    };
    
    
    /**
     * Output port.
     */
    public final transient DefaultOutputPort<Map<String, Object>> outport = 
        new DefaultOutputPort<Map<String, Object>>();
        
    @Override
    public void beginWindow(long arg0)
    {
      tuples1 = new ArrayList<Map<String, Object>>();
      tuples2 = new ArrayList<Map<String, Object>>();
    }
    
    /**
     * Emit valid row join on output port.
     * @param tuple1 Tuple from table1.
     * @param tuple2 Tuple form table2.   
     */
    private void joinColumn(Map<String, Object> tuple1,
        Map<String, Object> tuple2)
    {  
      if ((tuple1 == null) || (tuple2 == null)) return;
      boolean isValidJoin = true;
      if (joinCondition != null) {
        isValidJoin = joinCondition.isValidJoin(tuple1,tuple2);
      }
      if (isValidJoin) {
        Map<String, Object> join = new HashMap<String, Object>(tuple1);
        join.putAll(tuple2);
        outport.emit(join);
      }
    }

    /**
     * Get value for joinCondition.
     * @return Condition
     */
    public Condition getJoinCondition()
    {
      return joinCondition;
    }

    /**
     * Set value for joinCondition.
     * @param joinCondition set value for joinCondition.
     */
    public void setJoinCondition(@NotNull Condition joinCondition)
    {
      this.joinCondition = joinCondition;
    }
    
}
