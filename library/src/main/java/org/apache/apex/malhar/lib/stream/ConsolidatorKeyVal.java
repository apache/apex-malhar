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
package org.apache.apex.malhar.lib.stream;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * <p>
 * An implementation of operator that Aggregates &lt;key,value&gt; input from five ports of different types into
 * &lt;key,array list map&gt; for each key. <br>
 * <p>
 * Operator emits key/array list map at end window, this end window operator. <br>
 * <br>
 * <b>StateFull : Yes</b>, values are collected over application window. <br>
 * <b>Partitions : No</b>, will collect wrong results. <br>
 * <br>
 * <b>Ports : </b><br>
 * <b>Input(s) : </b> 5 max input ports, 3 are optional. <br>
 * <br>
 * @displayName Consolidator Key Value
 * @category Stream Manipulators
 * @tags arraylist, key value
 * @since 0.3.3
 */
@OperatorAnnotation(partitionable = false)
public class ConsolidatorKeyVal<K, V1, V2, V3, V4, V5> implements Operator
{
  /**
   * key/array values output result.
   */
  protected HashMap<K, ArrayList<Object>> result;

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  /**
   * <p>
   * Class operates on <K,V> pair, stores value in given number position in
   * list. <br>
   *
   * @param <V>
   *          value type.
   */
  public class ConsolidatorInputPort<V> extends
      DefaultInputPort<KeyValPair<K, V>>
  {
    /**
     * Value position in list.
     */
    private int number;

    /**
     * Constructor
     *
     * @param oper
     *          Connected operator.
     * @param num
     *          Value position in list.
     */
    ConsolidatorInputPort(Operator oper, int num)
    {
      super();
      number = num;
    }

    /**
     * Process key/value pair.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      ArrayList<Object> list = getObject(key);
      list.set(number, tuple.getValue());
    }

  }

  /**
   * V1 type value input port.
   */
  public final transient ConsolidatorInputPort<V1> in1 = new ConsolidatorInputPort<V1>(
      this, 0);

  /**
   * V2 type value input port.
   */
  public final transient ConsolidatorInputPort<V2> in2 = new ConsolidatorInputPort<V2>(
      this, 1);

  /**
   * V3 type value input port.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient ConsolidatorInputPort<V3> in3 = new ConsolidatorInputPort<V3>(
      this, 2);

  /**
   * V4 type value input port.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient ConsolidatorInputPort<V4> in4 = new ConsolidatorInputPort<V4>(
      this, 3);

  /**
   * V5 type value input port.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient ConsolidatorInputPort<V5> in5 = new ConsolidatorInputPort<V5>(
      this, 4);

  /**
   * Output port that emits a hashmap of &lt;key,arraylist&gt;.
   */
  public final transient DefaultOutputPort<HashMap<K, ArrayList<Object>>> out = new DefaultOutputPort<HashMap<K, ArrayList<Object>>>();

  /**
   * Get array list object for given key
   *
   * @param k  key
   * @return array list for key.
   */
  public ArrayList<Object> getObject(K k)
  {
    ArrayList<Object> val = result.get(k);
    if (val == null) {
      val = new ArrayList<Object>(5);
      val.add(0, null);
      val.add(1, null);
      val.add(2, null);
      val.add(3, null);
      val.add(4, null);
      result.put(k, val);
    }
    return val;
  }

  @Override
  public void beginWindow(long windowId)
  {
    result = new HashMap<K, ArrayList<Object>>();
  }

  /**
   * Emits merged data
   */
  @Override
  public void endWindow()
  {
    if (!result.isEmpty()) {
      out.emit(result);
    }
  }
}
