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

import org.apache.apex.malhar.lib.util.BaseNumberKeyValueOperator;
import org.apache.apex.malhar.lib.util.UnifierHashMapInteger;
import org.apache.apex.malhar.lib.util.UnifierHashMapSumKeys;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableInt;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Emits the sum and count of values for each key at the end of window.
 * <p>
 * Application accumulate sum across streaming window by setting cumulative flag
 * to true. <br>
 * This is an end of window operator<br>
 * <br>
 * <b>StateFull : Yes</b>, Sum is computed over application window and streaming
 * window. <br>
 * <b>Partitions : Yes</b>, Sum is unified at output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>sum</b>: emits HashMap&lt;K,V&gt;<br>
 * <b>count</b>: emits HashMap&lt;K,Integer&gt;</b><br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <b>cumulative</b>: boolean flag, if set the sum is not cleared at the end of
 * window, <br>
 * hence generating cumulative sum across streaming windows. Default is false.<br>
 * <br>
 * @displayName Sum Count Map
 * @category Math
 * @tags  number, sum, counting, map
 * @since 0.3.3
 * @deprecated
 */
@Deprecated
public class SumCountMap<K, V extends Number> extends
    BaseNumberKeyValueOperator<K, V>
{
  /**
   * Key/double sum map.
   */
  protected HashMap<K, MutableDouble> sums = new HashMap<K, MutableDouble>();

  /**
   * Key/integer sum map.
   */
  protected HashMap<K, MutableInt> counts = new HashMap<K, MutableInt>();

  /**
   * Cumulative sum flag.
   */
  protected boolean cumulative = false;

  /**
   * Input port that takes a map.&nbsp; It adds the values for each key and counts the number of occurrences for each key.
   */
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * For each tuple (a HashMap of keys,val pairs) Adds the values for each
     * key, Counts the number of occurrences of each key
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K, V> e : tuple.entrySet()) {
        K key = e.getKey();
        if (!doprocessKey(key)) {
          continue;
        }
        if (sum.isConnected()) {
          MutableDouble val = sums.get(key);
          if (val == null) {
            val = new MutableDouble(e.getValue().doubleValue());
          } else {
            val.add(e.getValue().doubleValue());
          }
          sums.put(cloneKey(key), val);
        }
        if (SumCountMap.this.count.isConnected()) {
          MutableInt count = counts.get(key);
          if (count == null) {
            count = new MutableInt(0);
            counts.put(cloneKey(key), count);
          }
          count.increment();
        }
      }
    }
  };

  /**
   * Key,sum map output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<HashMap<K, V>> sum = new DefaultOutputPort<HashMap<K, V>>()
  {
    @Override
    public Unifier<HashMap<K, V>> getUnifier()
    {
      return new UnifierHashMapSumKeys<K, V>();
    }
  };

  /**
   * Key,double sum map output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<HashMap<K, Double>> sumDouble = new DefaultOutputPort<HashMap<K, Double>>()
  {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Unifier<HashMap<K, Double>> getUnifier()
    {
      UnifierHashMapSumKeys ret = new UnifierHashMapSumKeys<K, Double>();
      ret.setType(Double.class);
      return ret;
    }
  };

  /**
   * Key,integer sum output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> sumInteger = new DefaultOutputPort<HashMap<K, Integer>>()
  {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Unifier<HashMap<K, Integer>> getUnifier()
    {
      UnifierHashMapSumKeys ret = new UnifierHashMapSumKeys<K, Integer>();
      ret.setType(Integer.class);
      return ret;
    }
  };


        /**
   * Key,long sum output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<HashMap<K, Long>> sumLong = new DefaultOutputPort<HashMap<K, Long>>()
  {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Unifier<HashMap<K, Long>> getUnifier()
    {
      UnifierHashMapSumKeys ret = new UnifierHashMapSumKeys<K, Long>();
      ret.setType(Long.class);
      return ret;
    }
  };

        /**
   * Key,short sum output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<HashMap<K, Short>> sumShort = new DefaultOutputPort<HashMap<K, Short>>()
  {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Unifier<HashMap<K, Short>> getUnifier()
    {
      UnifierHashMapSumKeys ret = new UnifierHashMapSumKeys<K, Short>();
      ret.setType(Short.class);
      return ret;
    }
  };

        /**
   * Key,float sum output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<HashMap<K, Float>> sumFloat = new DefaultOutputPort<HashMap<K, Float>>()
  {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Unifier<HashMap<K, Float>> getUnifier()
    {
      UnifierHashMapSumKeys ret = new UnifierHashMapSumKeys<K, Float>();
      ret.setType(Float.class);
      return ret;
    }
  };

        /**
   * Key,integer sum output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> count = new DefaultOutputPort<HashMap<K, Integer>>()
  {
    @Override
    public Unifier<HashMap<K, Integer>> getUnifier()
    {
      return new UnifierHashMapInteger<K>();
    }
  };

  /**
   * Get cumulative flag.
   *
   * @return cumulative flag
   */
  public boolean isCumulative()
  {
    return cumulative;
  }

  /**
   * set cumulative flag.
   *
   * @param cumulative
   *          input flag
   */
  public void setCumulative(boolean cumulative)
  {
    this.cumulative = cumulative;
  }

  /**
   * Emits on all ports that are connected. Data is precomputed during process
   * on input port endWindow just emits it for each key Clears the internal data
   * before return
   */
  @Override
  public void endWindow()
  {

    // Should allow users to send each key as a separate tuple to load balance
    // This is an aggregate node, so load balancing would most likely not be
    // needed

    HashMap<K, V> tuples = new HashMap<K, V>();
    HashMap<K, Integer> ctuples = new HashMap<K, Integer>();
    HashMap<K, Double> dtuples = new HashMap<K, Double>();
    HashMap<K, Integer> ituples = new HashMap<K, Integer>();
    HashMap<K, Float> ftuples = new HashMap<K, Float>();
    HashMap<K, Long> ltuples = new HashMap<K, Long>();
    HashMap<K, Short> stuples = new HashMap<K, Short>();

    for (Map.Entry<K, MutableDouble> e : sums.entrySet()) {
      K key = e.getKey();
      MutableDouble val = e.getValue();
      tuples.put(key, getValue(val.doubleValue()));
      dtuples.put(key, val.doubleValue());
      ituples.put(key, val.intValue());
      ftuples.put(key, val.floatValue());
      ltuples.put(key, val.longValue());
      stuples.put(key, val.shortValue());
      // ctuples.put(key, counts.get(e.getKey()).toInteger());
      MutableInt c = counts.get(e.getKey());
      if (c != null) {
        ctuples.put(key, c.toInteger());
      }
    }

    sum.emit(tuples);
    sumDouble.emit(dtuples);
    sumInteger.emit(ituples);
    sumLong.emit(ltuples);
    sumShort.emit(stuples);
    sumFloat.emit(ftuples);
    count.emit(ctuples);
    clearCache();
  }

  /**
   * Clear sum maps.
   */
  private void clearCache()
  {
    if (!cumulative) {
      sums.clear();
      counts.clear();
    }
  }
}
