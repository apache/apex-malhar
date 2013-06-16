/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.lib.util.BaseMatchOperator;
import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * A compare operation is done on input tuple based on the property "key", "value", and "cmp". All tuples
 * are emitted (inclusive) once a match is made. The comparison is done by getting double value from the Number.<p>
 * This module is a pass through<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>allafter</b>: emits Map&lt;K,V extends Number&gt; if compare function returns true<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp</b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * <b>Specific compile time checks</b>:<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AllAfterMatchMap&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>15 Million K,V pairs/s</b></td><td>All tuples after the first match</td><td>In-bound is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); key=a; value=3; cmp=eq</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for AllAfterMatchMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(Map&lt;K,V&gt;)</th><th><i>allafter</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=5,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,h=20,c=2}</td><td>{a=3,h=20,c=2}</td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td>{d=55,b=12}</td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td>{d=22}</td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td>{d=14}</td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2}</td><td>{d=46,e=2}</td></tr>
 * <tr><td>Data (process())</td><td>{d=1,d=5,d=4}</td><td>{d=1,d=5,d=4}</td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=23}</td><td>{d=4,a=23}</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class AllAfterMatchMap<K, V extends Number> extends BaseMatchOperator<K,V>
{
  @InputPortFieldAnnotation(name="data")
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>(this)
  {
    /**
     * Process HashMap<K,V> and emit all tuples at and after match
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      if (doemit) {
        allafter.emit(cloneTuple(tuple));
        return;
      }
      V v = tuple.get(getKey());
      if (v == null) { // error tuple
        return;
      }
      if (compareValue(v.doubleValue())) {
        doemit = true;
        allafter.emit(cloneTuple(tuple));
      }
    }
  };

  @OutputPortFieldAnnotation(name="allafter")
  public final transient DefaultOutputPort<HashMap<K, V>> allafter = new DefaultOutputPort<HashMap<K, V>>(this);
  boolean doemit = false;

  /**
   * Resets the matched variable
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    doemit = false;
  }
}
