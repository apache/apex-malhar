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
package org.apache.apex.malhar.contrib.misc.math;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.Min;

import org.apache.apex.malhar.lib.util.BaseNumberKeyValueOperator;
import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * Add all the values for each key on "numerator" and "denominator" and emits quotient at end of window for all keys in the denominator.
 * <p>
 * <br>
 * Application can set multiplication value for quotient(default = 1). <br>
 * Operator will calculate quotient of occurrence of key in numerator divided by
 * occurrence of key in denominator if countKey flag is true. <br>
 * Application can allow or block keys by setting filter key and inverse flag. <br>
 * <br>
 * <b>StateFull : Yes</b>, numerator/denominator values are summed over
 * application window. <br>
 * <b>Partitions : No, </b>, will yield wrong results, since values are summed
 * over app window. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>numerator</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>denominator</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>quotient</b>: emits HashMap&lt;K,Double&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse :</b> if set to true the key in the filter will block tuple<br>
 * <b>filterBy :</b> List of keys to filter on<br>
 * <b>countkey :</b> Get quotient of occurrence of keys in numerator and
 * denominator. <br>
 * <b>mult_by :</b> Set multiply by constant value. <br>
 * <br>
 * @displayName Quotient Map
 * @category Math
 * @tags division, sum, map
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
@OperatorAnnotation(partitionable = false)
public class QuotientMap<K, V extends Number> extends
    BaseNumberKeyValueOperator<K, V>
{
  /**
   * Numerator key/sum value map.
   */
  protected HashMap<K, MutableDouble> numerators = new HashMap<K, MutableDouble>();

  /**
   * Denominator key/sum value map.
   */
  protected HashMap<K, MutableDouble> denominators = new HashMap<K, MutableDouble>();

  /**
   * Count occurrence of keys if set to true.
   */
  boolean countkey = false;

  /**
   * Quotient multiply by value.
   */
  int mult_by = 1;

  /**
   * Numerator input port.
   */
  public final transient DefaultInputPort<Map<K, V>> numerator = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Added tuple to the numerator hash
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      addTuple(tuple, numerators);
    }
  };

  /**
   * Denominator input port.
   */
  public final transient DefaultInputPort<Map<K, V>> denominator = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Added tuple to the denominator hash
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      addTuple(tuple, denominators);
    }
  };

  /**
   * Quotient output port.
   */
  public final transient DefaultOutputPort<HashMap<K, Double>> quotient = new DefaultOutputPort<HashMap<K, Double>>();

  /**
   * Add tuple to nval/dval map.
   *
   * @param tuple
   *          key/value map on input port.
   * @param map
   *          key/summed value map.
   */
  public void addTuple(Map<K, V> tuple, Map<K, MutableDouble> map)
  {
    for (Map.Entry<K, V> e : tuple.entrySet()) {
      addEntry(e.getKey(), e.getValue(), map);
    }
  }

  /**
   * Add/Update entry to key/sum value map.
   *
   * @param key
   *          name.
   * @param value
   *          value for key.
   * @param map
   *          numerator/denominator key/sum map.
   */
  public void addEntry(K key, V value, Map<K, MutableDouble> map)
  {
    if (!doprocessKey(key) || (value == null)) {
      return;
    }
    MutableDouble val = map.get(key);
    if (val == null) {
      if (countkey) {
        val = new MutableDouble(1.00);
      } else {
        val = new MutableDouble(value.doubleValue());
      }
    } else {
      if (countkey) {
        val.increment();
      } else {
        val.add(value.doubleValue());
      }
    }
    map.put(cloneKey(key), val);
  }

  /**
   * getter for mult_by
   *
   * @return mult_by
   */

  @Min(0)
  public int getMult_by()
  {
    return mult_by;
  }

  /**
   * getter for countkey
   *
   * @return countkey
   */
  public boolean getCountkey()
  {
    return countkey;
  }

  /**
   * Setter for mult_by
   *
   * @param i
   */
  public void setMult_by(int i)
  {
    mult_by = i;
  }

  /**
   * setter for countkey
   *
   * @param i
   *          sets countkey
   */
  public void setCountkey(boolean i)
  {
    countkey = i;
  }

  /**
   * Generates tuples for each key and emits them. Only keys that are in the
   * denominator are iterated on If the key is only in the numerator, it gets
   * ignored (cannot do divide by 0) Clears internal data
   */
  @Override
  public void endWindow()
  {
    HashMap<K, Double> tuples = new HashMap<K, Double>();
    for (Map.Entry<K, MutableDouble> e : denominators.entrySet()) {
      MutableDouble nval = numerators.get(e.getKey());
      if (nval == null) {
        tuples.put(e.getKey(), new Double(0.0));
      } else {
        tuples.put(e.getKey(), new Double((nval.doubleValue() / e.getValue()
            .doubleValue()) * mult_by));
      }
    }
    if (!tuples.isEmpty()) {
      quotient.emit(tuples);
    }
    numerators.clear();
    denominators.clear();
  }
}
