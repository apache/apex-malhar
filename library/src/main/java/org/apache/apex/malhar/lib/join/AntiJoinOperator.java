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
package org.apache.apex.malhar.lib.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.streamquery.condition.Condition;
import org.apache.apex.malhar.lib.streamquery.index.Index;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * An implementation of Operator that reads table row data from two table data input ports. <br>
 * <p>
 * Operator anti-joins row on given condition and selected names, emits
 * anti-joined result at output port.
 * <br>
 * <b>StateFull : Yes,</b> Operator aggregates input over application window. <br>
 * <b>Partitions : No, </b> will yield wrong result(s). <br>
 * <br>
 * <b>Ports : </b> <br>
 * <b> inport1 : </b> Input port for table 1, expects HashMap&lt;String, Object&gt; <br>
 * <b> inport2 : </b> Input port for table 2, expects HashMap&lt;String, Object&gt; <br>
 * <b> outport : </b> Output anti-joined row port, emits HashMap&lt;String, ArrayList&lt;Object&gt;&gt; <br>
 * <br>
 * <b> Properties : </b>
 * <b> joinCondition : </b> Join condition for table rows. <br>
 * <b> table1Columns : </b> Columns to be selected from table1. <br>
 * <b> table2Columns : </b> Columns to be selected from table2. <br>
 * <br>
 *
 * @displayName Anti join
 * @category Stream Manipulators
 * @tags sql, anti join operator
 * @since 0.3.3
 */
@OperatorAnnotation(partitionable = false)
@Evolving
public class AntiJoinOperator implements Operator
{

  /**
   * Join Condition;
   */
  private Condition joinCondition;

  /**
   * Table1 select columns.
   * Note: only left table (Table1) will be output in an Anti-join
   */
  private ArrayList<Index> table1Columns = new ArrayList<>();

  /**
   * Collect data rows from input port 1.
   */
  private List<Map<String, Object>> table1;

  /**
   * Collect data from input port 2.
   */
  private List<Map<String, Object>> table2;

  /**
   * Input port 1 that takes a map of &lt;string,object&gt;.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport1 = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      table1.add(tuple);
      for (int j = 0; j < table2.size(); j++) {
        if ((joinCondition != null) && (joinCondition.isValidJoin(tuple, table2.get(j)))) {
          table1.remove(tuple);
        }
      }
    }
  };

  /**
   * Input port 2 that takes a map of &lt;string,object&gt;.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport2 = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      table2.add(tuple);

      for (int j = 0; j < table1.size(); j++) {
        if ((joinCondition != null)
            && (joinCondition.isValidJoin(table1.get(j), tuple))) {
          table1.remove(table1.get(j));
        }
      }
    }
  };

  /**
   * Output port that emits a map of &lt;string,object&gt;.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport = new DefaultOutputPort<>();

  @Override
  public void setup(OperatorContext arg0)
  {
    table1 = new ArrayList<>();
    table2 = new ArrayList<>();
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long arg0)
  {
  }

  @Override
  public void endWindow()
  {
    /* All joined rows have been removed
     * The ones left are the anti-joined result
     */
    for (int i = 0; i < table1.size(); i++) {
      joinRows(table1.get(i));
    }

    table1.clear();
    table2.clear();
  }

  /**
   * @return the joinCondition
   */
  public Condition getJoinCondition()
  {
    return joinCondition;
  }

  /**
   * Pick the supported condition. Currently only equal join is supported.
   *
   * @param joinCondition - join condition
   */
  public void setJoinCondition(Condition joinCondition)
  {
    this.joinCondition = joinCondition;
  }

  /**
   * Select table1 column name.
   */
  public void selectTable1Column(Index column)
  {
    table1Columns.add(column);
  }

  /**
   * Join row from table1 and table2.
   */
  protected void joinRows(Map<String, Object> row)
  {
    // joined row
    Map<String, Object> join = new HashMap<>();

    // filter table1 columns
    if (row != null) {
      for (Index index : table1Columns) {
        index.filter(row, join);
      }
    }

    // emit row
    outport.emit(join);
  }
}
