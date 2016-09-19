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
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.Unifier;

/**
 *  An implementation of Operator that provides sql order by operator semantic over live stream data. <br>
 * <p>
 * Input data rows are ordered by order rules, ordered result is emitted on output port. <br>
 * <br>
 *  *  <br>
 *  <b>StateFull : Yes,</b> Operator aggregates input over application window. <br>
 *  <b>Partitions : Yes, </b> This operator is also unifier on output port. <br>
 *  <br>
 * <b>Ports</b>:<br>
 * <b> inport : </b> Input hash map(row) port, expects HashMap&lt;String,Object&gt;<<br>
 * <b> outport : </b> Output hash map(row) port, emits  HashMap&lt;String,Object&gt;<br>
 * <br>
 * <b> Properties : </b> <br>
 * <b> orderByRules : </b>List of order by rules for tuples.
 * @displayName OrderBy
 * @category Stream Manipulators
 * @tags orderby operator
 * @since 0.3.5
 * @deprecated
 */
@Deprecated
public class OrderByOperator implements Operator, Unifier<Map<String, Object>>
{
  /**
   * Order by rules.
   */
  ArrayList<OrderByRule<?>> orderByRules = new ArrayList<OrderByRule<?>>();

  /**
   * Descending flag.
   */
  private boolean isDescending;

  /**
   * collected rows.
   */
  private ArrayList<Map<String, Object>> rows;

  /**
   * Add order by rule.
   */
  public void addOrderByRule(OrderByRule<?> rule)
  {
    orderByRules.add(rule);
  }

  /**
   * @return isDescending
   */
  public boolean isDescending()
  {
    return isDescending;
  }

  /**
   * @param isDescending isDescending
   */
  public void setDescending(boolean isDescending)
  {
    this.isDescending = isDescending;
  }

  @Override
  public void process(Map<String, Object> tuple)
  {
    rows.add(tuple);
  }

  @Override
  public void beginWindow(long arg0)
  {
    rows = new ArrayList<Map<String, Object>>();
  }

  @Override
  public void endWindow()
  {
    for (int i = 0; i < orderByRules.size(); i++) {
      rows = orderByRules.get(i).sort(rows);
    }
    if (isDescending) {
      for (int i = 0; i < rows.size(); i++) {
        outport.emit(rows.get(i));
      }
    } else {
      for (int i = rows.size() - 1; i >= 0; i--) {
        outport.emit(rows.get(i));
      }
    }
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  /**
   * Input port that takes a map of &lt;string,object&gt;.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      rows.add(tuple);
    }
  };

  /**
   * Output port that emits a map of &lt;string,object&gt;.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport = new DefaultOutputPort<Map<String, Object>>()
  {
    @Override
    public Unifier<Map<String, Object>> getUnifier()
    {
      OrderByOperator unifier = new OrderByOperator();
      for (int i = 0; i < getOrderByRules().size(); i++) {
        unifier.addOrderByRule(getOrderByRules().get(i));
      }
      unifier.setDescending(isDescending);
      return unifier;
    }
  };

  /**
   * @return the orderByRules
   */
  public ArrayList<OrderByRule<?>> getOrderByRules()
  {
    return orderByRules;
  }

  /**
   * The order by rules used to order incoming tuples.
   * @param oredrByRules the orderByRules to set
   */
  public void setOrderByRules(ArrayList<OrderByRule<?>> oredrByRules)
  {
    this.orderByRules = oredrByRules;
  }
}
