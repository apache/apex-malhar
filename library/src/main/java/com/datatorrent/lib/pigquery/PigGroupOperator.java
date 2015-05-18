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
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.pigquery.condition.PigGroupCondition;
import com.datatorrent.lib.util.UnifierMap;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;


/**
 * An implementation of BaseOperator that implements Apache Pig Group operator semantic. <br>
 * <p>
 * This operator does not support Group All semantic. <br>
 * <pre>
 * B = GROUP A BY age;
 *
 * DESCRIBE B;
 * B: {group: int, A: {name: chararray,age: int,gpa: float}}
 *
 * ILLUSTRATE B;
 * etc ...
 * ----------------------------------------------------------------------
 * | B     | group: int | A: bag({name: chararray,age: int,gpa: float}) |
 * ----------------------------------------------------------------------
 * |       | 18         | {(John, 18, 4.0), (Joe, 18, 3.8)}             |
 * |       | 20         | {(Bill, 20, 3.9)}                             |
 * ----------------------------------------------------------------------
 *
 * DUMP B;
 * (18,{(John,18,4.0F),(Joe,18,3.8F)})
 * (19,{(Mary,19,3.8F)})
 * (20,{(Bill,20,3.9F)})
 * </pre>
 * <b>Ports : </b> <br>
 * <b> inport : </b> expects tuple Map<String, Object> <br>
 * <b>outport : </b> emits Map<Object, List<Map<String, Object>>> <br>
 *
 * <b>StateFull : </b> Yes, tuples are aggregated over application window. <br>
 * <b>Partitions : </b> Yes, map unifier on output port. <br>
 *
 * <b>Properties : </b> <br>
 * <b>groupByCondition : </b> Group condition. <br>
 * <br>
 * @displayName Pig Group
 * @category Pig Query
 * @tags map, string, group operator, condition
 * @since 0.3.4
 */
@Deprecated
public class PigGroupOperator  extends BaseOperator
{
  /**
   * Aggregate tuple list.
   */
  private ArrayList<Map<String, Object>> tuples;
  
  /**
   * Group by condition.
   */
  @NotNull
  private PigGroupCondition groupByCondition;
  
  public PigGroupOperator(@NotNull PigGroupCondition groupByCondition) {
    this.groupByCondition = groupByCondition;
  }
  
  /**
   * Input port that takes map of &lt;String, Object&gt.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      tuples.add(tuple);
    }
  };
  
  /**
   * Output port that emits a map of &lt;Object, List&lt;Map&lt;String, Object&gt;&gt;&gt;.
   */
  public final transient DefaultOutputPort<Map<Object, List<Map<String, Object>>>> outport = 
      new DefaultOutputPort<Map<Object, List<Map<String, Object>>>>()
  {
    @Override
    public Unifier<Map<Object, List<Map<String, Object>>>> getUnifier()
    {
      return new UnifierMap<Object, List<Map<String, Object>>>();
    }
  };

  /**
   * Get value for groupByCondition.
   * @return GroupByCondition
   */
  public PigGroupCondition getGroupByCondition()
  {
    return groupByCondition;
  }

  /**
   * Set value for groupByCondition.
   * @param groupByCondition set value for groupByCondition.
   */
  public void setGroupByCondition(@NotNull PigGroupCondition groupByCondition)
  {
    this.groupByCondition = groupByCondition;
  }
  
  @Override
  public void beginWindow(long arg0)
  {
    tuples = new ArrayList<Map<String, Object>>();
  }

  @Override
  public void endWindow()
  {
    Map<Object, List<Map<String, Object>>> result = new HashMap<Object, List<Map<String, Object>>>();
    for (Map<String, Object> tuple : tuples) {
      Object key = groupByCondition.compute(tuple);
      List<Map<String, Object>> list;
      if (result.containsKey(key)) {
        list = result.get(key);
      } else {
        list = new ArrayList<Map<String, Object>>();
        result.put(key, list);
      }
      list.add(tuple);
    }
    outport.emit(result);
  }
}
