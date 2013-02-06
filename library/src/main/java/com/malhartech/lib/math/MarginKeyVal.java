/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.StreamCodec;
import com.malhartech.lib.util.BaseNumberKeyValueOperator;
import com.malhartech.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableDouble;

/**
 *
 * Adds all values for each key in "numerator" and "denominator", and at the end of window emits the margin for each key
 * (1 - numerator/denominator). <p>
 * <br>The values are added for each key within the window and for each stream.<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>numerator</b>: expects KeyValPair&lt;K,V&gt;<br>
 * <b>denominator</b>: expects KeyValPair&lt;K,V&gt;<br>
 * <b>margin</b>: emits HashMap&lt;K,Double&gt;, one entry per key per window<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for MarginMap&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>37 Million K,V pairs/s</b></td><td>One tuple per key per window per port</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer) and percent set to true</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for MarginMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th colspan=2>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>numerator</i>(KeyValPair&lt;K,V&gt;)</th><th><i>denominator</i>(KeyValPair&lt;K,V&gt;)</th><th><i>margin</i>(KeyValPair&lt;K,Double&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td></td><td>{a=2,a=8}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=4000}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>{c=500,d=282}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>{b=7,e=3}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=23,g=5,h=44}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>{c=1500}</td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>{a=40,b=30}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>a=28<br>b=0<br>c=-100<br>d=50<br>e=33.3</td></tr>
 * </table>
 * <br>
 *
 * @author Locknath Shil <locknath@malhar-inc.com><br>
 * <br>
 */
public class MarginKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "numerator")
  public final transient DefaultInputPort<KeyValPair<K, V>> numerator = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    /**
     * Adds tuple to the numerator hash.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      addTuple(tuple, numerators);
    }

    /**
     * Set StreamCodec used for partitioning.
     */
    @Override
    public Class<? extends StreamCodec<KeyValPair<K, V>>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }
  };
  @InputPortFieldAnnotation(name = "denominator")
  public final transient DefaultInputPort<KeyValPair<K, V>> denominator = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    /**
     * Adds tuple to the denominator hash.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      addTuple(tuple, denominators);
    }

    /**
     * Set StreamCodec used for partitioning.
     */
    @Override
    public Class<? extends StreamCodec<KeyValPair<K, V>>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }
  };

  /**
   * Adds the value for each key.
   *
   * @param tuple
   * @param map
   */
  public void addTuple(KeyValPair<K, V> tuple, Map<K, MutableDouble> map)
  {
    K key = tuple.getKey();
    if (!doprocessKey(key) || (tuple.getValue() == null)) {
      return;
    }
    MutableDouble val = map.get(key);
    if (val == null) {
      val = new MutableDouble(0.0);
      map.put(cloneKey(key), val);
    }
    val.add(tuple.getValue().doubleValue());
  }
  @OutputPortFieldAnnotation(name = "margin")
  public final transient DefaultOutputPort<KeyValPair<K, V>> margin = new DefaultOutputPort<KeyValPair<K, V>>(this);

  protected transient HashMap<K, MutableDouble> numerators = new HashMap<K, MutableDouble>();
  protected transient HashMap<K, MutableDouble> denominators = new HashMap<K, MutableDouble>();
  protected boolean percent = false;

  /**
   * getter function for percent
   *
   * @return percent
   */
  public boolean getPercent()
  {
    return percent;
  }

  /**
   * setter function for percent
   *
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
    Double val;
    for (Map.Entry<K, MutableDouble> e: denominators.entrySet()) {
      K key = e.getKey();
      MutableDouble nval = numerators.get(key);
      if (nval == null) {
        nval = new MutableDouble(0.0);
      }
      else {
        numerators.remove(key); // so that all left over keys can be reported
      }
      if (percent) {
        val = (1 - nval.doubleValue() / e.getValue().doubleValue()) * 100;
      }
      else {
        val = 1 - nval.doubleValue() / e.getValue().doubleValue();
      }

      margin.emit(new KeyValPair(key, getValue(val.doubleValue())));
    }

    numerators.clear();
    denominators.clear();
  }
}
