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
package org.apache.apex.malhar.lib.math;

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.util.BaseNumberKeyValueOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Emits the sum of values for each key at the end of window.
 * <p>
 * This is an end window operator. Default unifier works as this operator follows sticky partition.<br> <br> <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br> <b>sum</b>: emits KeyValPair&lt;K,V extends Number&gt;<br> <br> <b>Properties</b>:<br> <b>inverse</b>: If set to true the key in the
 * filter will block tuple<br> <b>filterBy</b>: List of keys to filter on<br>
 * <b>cumulative</b>: boolean flag, if set the sum is not cleared at the end of window, <br>
 * hence generating cumulative sum
 * across streaming windows. Default is false.<br>
 * <br>
 * @displayName Sum Key Value
 * @category Math
 * @tags  numeric, sum, key value
 * @since 0.3.2
 */
public class SumKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  protected static class SumEntry
  {
    public MutableDouble sum;
    public boolean changed = true;

    SumEntry()
    {
    }

    SumEntry(MutableDouble sum, boolean changed)
    {
      this.sum = sum;
      this.changed = changed;
    }

  }

  /**
   * Sums key map.
   */
  protected HashMap<K, SumEntry> sums = new HashMap<K, SumEntry>();

  /**
   * Cumulative sum flag.
   */
  protected boolean cumulative = false;

  /**
   * Input port that takes key value pairs and adds the values for each key.
   */
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>()
  {
    /**
     * For each tuple (a key value pair) Adds the values for each key.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      if (!doprocessKey(key)) {
        return;
      }
      SumEntry val = sums.get(key);
      if (val == null) {
        val = new SumEntry(new MutableDouble(tuple.getValue().doubleValue()), true);
      } else {
        val.sum.add(tuple.getValue().doubleValue());
        val.changed = true;
      }
      sums.put(cloneKey(key), val);
    }

    /**
     * Stream codec used for partitioning.
     */
    @Override
    public StreamCodec<KeyValPair<K, V>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }

  };

  /**
   * Output sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, V>> sum = new DefaultOutputPort<KeyValPair<K, V>>();

  /**
   * Output double sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Double>> sumDouble = new DefaultOutputPort<KeyValPair<K, Double>>();

  /**
   * Output integer sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Integer>> sumInteger = new DefaultOutputPort<KeyValPair<K, Integer>>();

  /**
   * Output long sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Long>> sumLong = new DefaultOutputPort<KeyValPair<K, Long>>();

  /**
   * Output short sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Short>> sumShort = new DefaultOutputPort<KeyValPair<K, Short>>();

  /**
   * Output float sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Float>> sumFloat = new DefaultOutputPort<KeyValPair<K, Float>>();

  /**
   * Get cumulative flag.
   * @return cumulative flag.
   */
  public boolean isCumulative()
  {
    return cumulative;
  }

  /**
   *
   * @param cumulative
   */
  public void setCumulative(boolean cumulative)
  {
    this.cumulative = cumulative;
  }


  /**
   * Emits on all ports that are connected. Data is precomputed during process on input port and endWindow just emits it for each key. Clears the internal data.
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, SumEntry> e: sums.entrySet()) {
      K key = e.getKey();
      SumEntry val = e.getValue();
      if (val.changed) {
        sum.emit(new KeyValPair<K, V>(key, getValue(val.sum.doubleValue())));
        sumDouble.emit(new KeyValPair<K, Double>(key, val.sum.doubleValue()));
        sumInteger.emit(new KeyValPair<K, Integer>(key, val.sum.intValue()));
        sumFloat.emit(new KeyValPair<K, Float>(key, val.sum.floatValue()));
        sumShort.emit(new KeyValPair<K, Short>(key, val.sum.shortValue()));
        sumLong.emit(new KeyValPair<K, Long>(key, val.sum.longValue()));
      }
    }
    clearCache();
  }

  /**
   * Clears the cache making this operator stateless on window boundary
   */
  public void clearCache()
  {
    if (cumulative) {
      for (Map.Entry<K, SumEntry> e : sums.entrySet()) {
        SumEntry val = e.getValue();
        val.changed = false;
      }
    } else {
      sums.clear();
    }
  }

}
