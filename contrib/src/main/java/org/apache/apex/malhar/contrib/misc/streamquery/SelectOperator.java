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
import java.util.Map;

import org.apache.apex.malhar.lib.streamquery.condition.Condition;
import org.apache.apex.malhar.lib.streamquery.index.Index;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * An implementation of that provides sql select query semantics on live data stream. <br>
 * <p>
 * Stream rows passing condition are emitted on output port stream. <br>
 * <br>
 * <b>StateFull : NO,</b> all row data is processed in current time window. <br>
 * <b>Partitions : Yes, </b> No Input dependency among input rows. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b> inport : </b> Input hash map(row) port, expects
 * HashMap&lt;String,Object&gt;<<br>
 * <b> outport : </b> Output hash map(row) port, emits
 * HashMap&lt;String,Object&gt;<br>
 * <br>
 * <b> Properties : <b> <br>
 * <b> condition : </b> Select condition for selecting rows. <br>
 * <b> columns : </b> Column names/aggregate functions for select. <br>
 * <br>
 * @displayName Select
 * @category Stream Manipulators
 * @tags sql select operator, index, sql condition
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
public class SelectOperator extends BaseOperator
{

  /**
   * select columns/expression;
   */
  private ArrayList<Index> indexes = new ArrayList<Index>();

  /**
   * condition.
   */
  private Condition condition = null;

  /**
   * add index.
   */
  public void addIndex(Index index)
  {
    indexes.add(index);
  }

  /**
   * set condition.
   */
  public void setCondition(Condition condition)
  {
    this.condition = condition;
  }

  /**
   * Input port that takes a map of &lt;string,object&gt;.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport = new DefaultInputPort<Map<String, Object>>()
  {

    @Override
    public void process(Map<String, Object> tuple)
    {
      if ((condition != null) && (!condition.isValidRow(tuple))) {
        return;
      }
      if (indexes.size() == 0) {
        outport.emit(tuple);
        return;
      }
      Map<String, Object> result = new HashMap<String, Object>();
      for (int i = 0; i < indexes.size(); i++) {
        indexes.get(i).filter(tuple, result);
      }
      outport.emit(result);
    }
  };

  /**
   * Output port that emits a map of &lt;string,object&gt;.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport = new DefaultOutputPort<Map<String, Object>>();
}
