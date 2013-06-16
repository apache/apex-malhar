/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseNumberKeyValueOperator;
import com.malhartech.lib.util.UnifierHashMap;

import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.Min;
import org.apache.commons.lang.mutable.MutableDouble;

/**
 *
 * Add all the values for each key on "numerator" and "denominator" and emits quotient at end of window for all keys in the denominator<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>numerator</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>denominator</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>quotient</b>: emits HashMap&lt;K,Double&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <b>minCount</b>: Only emit tuple if
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Quotien&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>18 Million K,V pairs/s</b></td><td>One K,V pair per key per window</td><td>In-bound is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for QuotientMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th colspan=2>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>numerator</i>(Map&lt;K,V&gt;)</th><th><i>denominator</i>(Map&lt;K,V&gt;)</th><th><i>quotient</i>(HashMap&lt;K,Double&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>{a=5,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>{a=5,h=20,c=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>{d=1,d=5,d=4}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=23}</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>{a=3.6,b=7.4,c=500.0,d=14.1,h=0.0}</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class QuotientMap<K, V extends Number> extends BaseNumberKeyValueOperator<K,V>
{
  @InputPortFieldAnnotation(name = "numerator")
  public final transient DefaultInputPort<Map<K, V>> numerator = new DefaultInputPort<Map<K, V>>(this)
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
  @InputPortFieldAnnotation(name = "denominator")
  public final transient DefaultInputPort<Map<K, V>> denominator = new DefaultInputPort<Map<K, V>>(this)
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
  @OutputPortFieldAnnotation(name = "quotient")
  public final transient DefaultOutputPort<HashMap<K, Double>> quotient = new DefaultOutputPort<HashMap<K, Double>>(this);

  public void addTuple(Map<K, V> tuple, Map<K, MutableDouble> map)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      addEntry(e.getKey(), e.getValue(), map);
    }
  }

  public void addEntry(K key, V value, Map<K, MutableDouble> map)
  {
    if (!doprocessKey(key) || (value == null)) {
      return;
    }
    MutableDouble val = map.get(key);
    if (val == null) {
      val = new MutableDouble(value.doubleValue());
    }
    else {
      if (countkey) {
        val.increment();
      }
      else {
        val.add(value.doubleValue());
      }
    }
    map.put(cloneKey(key), val);
  }

  protected HashMap<K, MutableDouble> numerators = new HashMap<K, MutableDouble>();
  protected HashMap<K, MutableDouble> denominators = new HashMap<K, MutableDouble>();
  boolean countkey = false;
  int mult_by = 1;

  /**
   * getter for mult_by
   * @return mult_by
   */

  @Min(0)
  public int getMult_by()
  {
    return mult_by;
  }

  /**
   * getter for countkey
   * @return countkey
   */
  public boolean getCountkey()
  {
    return countkey;
  }


  /**
   * Setter for mult_by
   * @param i
   */
  public void setMult_by(int i)
  {
    mult_by = i;
  }

  /**
   * setter for countkey
   * @param i sets countkey
   */
  public void setCountkey(boolean i)
  {
    countkey = i;
  }

  /**
   * Generates tuples for each key and emits them. Only keys that are in the denominator are iterated on
   * If the key is only in the numerator, it gets ignored (cannot do divide by 0)
   * Clears internal data
   */
  @Override
  public void endWindow()
  {
    HashMap<K, Double> tuples = new HashMap<K, Double>();
    for (Map.Entry<K, MutableDouble> e: denominators.entrySet()) {
      MutableDouble nval = numerators.get(e.getKey());
      if (nval == null) {
        tuples.put(e.getKey(), new Double(0.0));
      }
      else {
        tuples.put(e.getKey(), new Double((nval.doubleValue() / e.getValue().doubleValue()) * mult_by));
      }
    }
    if (!tuples.isEmpty()) {
      quotient.emit(tuples);
    }
    numerators.clear();
    denominators.clear();
  }
}
