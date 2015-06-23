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

import java.util.Map;

import com.datatorrent.lib.streamquery.condition.Condition;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;


/**
 * An implementation of BaseOperator that implements Apache Pig filter statement semantic. <br>
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
 * @displayName Pig Filter
 * @category Pig Query
 * @tags map, string, filter operator
 * @since 0.3.4
 */
@Deprecated
public class PigFilterOperator  extends BaseOperator
{
  /**
   * condition.
   */
  private Condition condition = null;

  /**
   * Get value for condition.
   * @return Condition
   */
  public Condition getCondition()
  {
    return condition;
  }

  /**
   * Set value for condition.
   * @param condition set value for condition.
   */
  public void setCondition(Condition condition)
  {
    this.condition = condition;
  }
  
  /**
   * Input port that takes map of &lt;String, Object&gt.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport = new DefaultInputPort<Map<String, Object>>()
  {

    @Override
    public void process(Map<String, Object> tuple)
    {
      if ((condition != null) && (!condition.isValidRow(tuple))) return;
      outport.emit(tuple);
    }
  };
  
  /**
   * Output port that emits a map of &lt;String, Object&gt.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport = new DefaultOutputPort<Map<String, Object>>();
}
