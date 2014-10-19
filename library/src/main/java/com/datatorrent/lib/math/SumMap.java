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
import com.datatorrent.lib.util.UnifierHashMapSumKeys;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;

/**
 * Emits the sum of values for each key at the end of window.
 * <p>
 * This is an end of window operator<br>
 * <br>
 * <b>StateFull : Yes</b>, sum is computed over application window. <br>
 * <b>Partitions : Yes</b>, sum is unified at output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>sum</b>: emits HashMap&lt;K,V&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <b>cumulative</b>: boolean flag, if set the sum is not cleared at the end of window, <br>
 * hence generating cumulative sum across streaming windows. Default is false.<br>
 * <br>
 * @displayName Sum Map
 * @category Math
 * @tags  numeric, sum, map
 * @since 0.3.2
 */
public class SumMap<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
	/**
	 * key/sums map.
	 */
  protected HashMap<K, MutableDouble> sums = new HashMap<K, MutableDouble>();
  
  /**
   * cumulative flag.
   */
  protected boolean cumulative = false;
  
  /**
   * Input port that takes a map and adds the values for each key.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * For each tuple (a HashMap of keys,val pairs)
     * Adds the values for each key.
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        K key = e.getKey();
        if (!doprocessKey(key)) {
          continue;
        }
        if (sum.isConnected()) {
          MutableDouble val = sums.get(key);
          if (val == null) {
            val = new MutableDouble(e.getValue().doubleValue());
          }
          else {
            val.add(e.getValue().doubleValue());
          }
          sums.put(cloneKey(key), val);
        }
      }
    }
  };
  
  /**
   * Sum output port.
   */
  @OutputPortFieldAnnotation(name = "sum", optional = true)
  public final transient DefaultOutputPort<HashMap<K, V>> sum = new DefaultOutputPort<HashMap<K, V>>()
  {
    @Override
    public Unifier<HashMap<K, V>> getUnifier()
    {
      return new UnifierHashMapSumKeys<K, V>();
    }
  };

  /**
   * Double sum output port.
   */
  @OutputPortFieldAnnotation(name = "sumDouble", optional = true)
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
   * Integer sum output port.
   */
  @OutputPortFieldAnnotation(name = "sumInteger", optional = true)
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
   * Long sum output port.
   */
  @OutputPortFieldAnnotation(name = "sumLong", optional = true)
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
   * Short sum output port.
   */
  @OutputPortFieldAnnotation(name = "sumShort", optional = true)
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
   * Float sum output port.
   */
  @OutputPortFieldAnnotation(name = "sumFloat", optional = true)
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
   * Get cumulative flag.
   * @return cumulative flag
   */
  public boolean isCumulative()
  {
    return cumulative;
  }

  /**
   * Set cumulative flag.
   * @param cumulative flag
   */
  public void setCumulative(boolean cumulative)
  {
    this.cumulative = cumulative;
  }

  /**
   * Emits on all ports that are connected. Data is precomputed during process on input port
   * endWindow just emits it for each key
   * Clears the internal data before return
   */
  @Override
  public void endWindow()
  {
    HashMap<K, V> tuples = new HashMap<K, V>();
    HashMap<K, Double> dtuples = new HashMap<K, Double>();
    HashMap<K, Integer> ituples = new HashMap<K, Integer>();
    HashMap<K, Float> ftuples = new HashMap<K, Float>();
    HashMap<K, Long> ltuples = new HashMap<K, Long>();
    HashMap<K, Short> stuples = new HashMap<K, Short>();

    for (Map.Entry<K, MutableDouble> e: sums.entrySet()) {
      K key = e.getKey();
      MutableDouble val = e.getValue();
      tuples.put(key, getValue(val.doubleValue()));
      dtuples.put(key, val.doubleValue());
      ituples.put(key, val.intValue());
      ltuples.put(key, val.longValue());
      stuples.put(key, val.shortValue());
      ftuples.put(key, val.floatValue());
    }
    sum.emit(tuples);
    sumDouble.emit(dtuples);
    sumInteger.emit(ituples);
    sumFloat.emit(ftuples);
    sumLong.emit(ltuples);
    sumShort.emit(stuples);
    clearCache();
  }

  /**
   * Clears the cache making this operator stateless on window boundary
   */
  public void clearCache()
  {
    if (!cumulative) {
      sums.clear();
    }
  }
}
