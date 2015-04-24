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

import com.datatorrent.lib.pigquery.generate.Generate;
import com.datatorrent.lib.util.UnifierMap;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;


/**
 * <p>An implementation of BaseOperator that implements Apache Pig ForEach Operator.</p>
 * @displayName Pig ForEach
 * @category Pig Query
 * @tags map, string, foreach operator
 * @since 0.3.4
 */
@Deprecated
public class PigForeachOperator extends BaseOperator
{
  /**
   * Generate indexes.
   */
  private ArrayList<Generate> generates = new ArrayList<Generate>();
  
  /**
   * Add foreach generate indexes.
   * @param index  Generate index to be added.
   */
  public void addGenerateIndex(Generate index)
  {
    generates.add(index);
  }
  
  /**
   * Input port that takes map of &lt;String, Object&gt.
   */
  public final transient DefaultInputPort<Map<String, Object>> inport = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      Map<String, Object> collect = new HashMap<String, Object>();
      for (Generate index : generates) {
        index.evaluate(tuple, collect);
      }
      outport.emit(collect);
    }
  };
  
  /**
   * Output port that emits a map of &lt;String, Object&gt.
   */
  public final transient DefaultOutputPort<Map<String, Object>> outport = 
      new DefaultOutputPort<Map<String, Object>>()
  {
    @Override
    public Unifier<Map<String, Object>> getUnifier()
    {
      return new UnifierMap<String, Object>();
    }
  };
}
