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
package com.datatorrent.lib.math;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.UnifierHashMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;


/**
 *
 * This operator adds all values for each key in "numerator" and "denominator", and at the end of window emits the margin for each key.Margin is calculated as  1 - SUM(numerator)/SUM(denominator).
 * <p>
 * <br>The values are added for each key within the window and for each stream.<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>numerator</b>: expects Map&lt;K,V&gt;<br>
 * <b>denominator</b>: expects Map&lt;K,V&gt;<br>
 * <b>margin</b>: emits HashMap&lt;K,Double&gt;, one entry per key per window<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * @displayname: Margin Map
 * @category: lib.math
 * @tags: sum, division, Number, Map, MutableDouble
 * @since 0.3.2
 */
public class MarginMap<K, V extends Number> extends BaseNumberKeyValueOperator<K,V>
{
  @InputPortFieldAnnotation(name = "numerator")
  public final transient DefaultInputPort<Map<K, V>> numerator = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Adds tuple to the numerator hash
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      addTuple(tuple, numerators);
    }
  };
  @InputPortFieldAnnotation(name = "denominator")
  public final transient DefaultInputPort<Map<K, V>> denominator = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Adds tuple to the denominator hash
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      addTuple(tuple, denominators);
    }
  };

  /**
   * Adds the value for each key.
   * @param tuple
   * @param map
   */
  public void addTuple(Map<K, V> tuple, Map<K, MutableDouble> map)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      if (!doprocessKey(e.getKey()) || (e.getValue() == null)) {
        continue;
      }
      MutableDouble val = map.get(e.getKey());
      if (val == null) {
        val = new MutableDouble(0.0);
        map.put(cloneKey(e.getKey()), val);
      }
      val.add(e.getValue().doubleValue());
    }
  }
  @OutputPortFieldAnnotation(name = "margin")
  public final transient DefaultOutputPort<HashMap<K, V>> margin = new DefaultOutputPort<HashMap<K, V>>()
  {
    @Override
    public Unifier<HashMap<K, V>> getUnifier()
    {
      return new UnifierHashMap<K,V>();
    }
  };

  protected HashMap<K, MutableDouble> numerators = new HashMap<K, MutableDouble>();
  protected HashMap<K, MutableDouble> denominators = new HashMap<K, MutableDouble>();
  boolean percent = false;

  /**
   * getter function for percent
   * @return percent
   */
  public boolean getPercent()
  {
    return percent;
  }

  /**
   * setter function for percent
   * @param val sets percent
   */
  public void setPercent(boolean val)
  {
    percent = val;
  }

  /**
   * Generates tuples for each key and emits them. Only keys that are in the denominator are iterated on
   * If the key is only in the numerator, it gets ignored (cannot do divide by 0)
   * Clears internal data
   */
  @Override
  public void endWindow()
  {
    HashMap<K, V> tuples = new HashMap<K, V>();
    Double val;
    for (Map.Entry<K, MutableDouble> e: denominators.entrySet()) {
      MutableDouble nval = numerators.get(e.getKey());
      if (nval == null) {
        nval = new MutableDouble(0.0);
      }
      else {
        numerators.remove(e.getKey()); // so that all left over keys can be reported
      }
      if (percent) {
        val = (1 - nval.doubleValue() / e.getValue().doubleValue()) * 100;
      }
      else {
        val = 1 - nval.doubleValue() / e.getValue().doubleValue();
      }
      tuples.put(e.getKey(), getValue(val.doubleValue()));
    }
    if (!tuples.isEmpty()) {
      margin.emit(tuples);
    }
    numerators.clear();
    denominators.clear();
  }
}




