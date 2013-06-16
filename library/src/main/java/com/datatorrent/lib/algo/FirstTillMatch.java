/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseMatchOperator;
import java.util.HashMap;

/**
 *
 * All key.val pairs with val sub-classed from Number are emitted till the first match;  A compare operation is done based on the property "key",
 * "value", and "cmp". Then on no tuple is emitted in that window. The comparison is done by getting double value of the Number.<p>
 * This module is a pass through<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input port, expects HashMap&lt;K,V&gt;<br>
 * <b>first</b>: Output port, emits HashMap&lt;K,V&gt; if compare function returns true<br>
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
 * <br>
 * <b>Specific run time checks are</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for FirstTillMatch&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 16 Million K,V pairs/s</b></td><td>All tuples till a matching key,val pair is found</td><td>In-bound throughput and the occurrence of
 * matching key,val pair are the main determinant of performance. Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); key=a; value=3; cmp=eq</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for FirstTillMatch&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>first</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td>{a=2,b=20,c=1000}</td></tr>
 * <tr><td>Data (process())</td><td>{a=-1}</td><td>{a=-1}</td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td>{a=10,b=5}</td></tr>
 * <tr><td>Data (process())</td><td>{a=3,b=-5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=2,d=14,h=20,c=2,b=-5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14,a=23,e=2,b=5}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class FirstTillMatch<K, V extends Number> extends BaseMatchOperator<K, V>
{
  @InputPortFieldAnnotation(name="data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * Compares the key,val pair with the match condition. Till a match is found tuples are emitted.
     * Once a match is found, state is set to emitted, and no more tuples are compared (no more emits).
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      if (emitted) {
        return;
      }
      V val = tuple.get(getKey());
      if (val == null) { // skip if the key does not exist
        return;
      }
      if (compareValue(val.doubleValue())) {
        emitted = true;
      }
      if (!emitted) {
        first.emit(cloneTuple(tuple));
      }
    }
  };

  @OutputPortFieldAnnotation(name="first")
  public final transient DefaultOutputPort<HashMap<K, V>> first = new DefaultOutputPort<HashMap<K, V>>(this);
  boolean emitted = false;

  /**
   * Emitted set is reset to false
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    emitted = false;
  }
}
