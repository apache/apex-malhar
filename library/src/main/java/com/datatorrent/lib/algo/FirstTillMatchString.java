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
 * All key,val pairs with val of type String are emitted till the first match;  A compare operation is done based on the property "key", "value",
 * and "cmp". Then on no tuple is emitted in that window. The comparison is done by getting double value of the Number.<p>
 * This module is a pass through<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input port, expects HashMap&lt;K,String&gt;<br>
 * <b>first</b>: Output port, emits HashMap&lt;K,String&gt; if compare function returns true<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp</b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * Compile time checks<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * <b>Specific compile time checks</b>:<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * <b>Specific run time checks are</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for FirstTillMatchString&lt;K,StringV&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 18 Million K,String pairs/s</b></td><td>All tuples till a matching key,String pair is found</td><td>In-bound throughput and the occurrence of
 * matching key,String pair are the main determinant of performance. K is assumed to be immutable. If K is mutable, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String); key=a; value=3; cmp=eq</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for FirstTillMatchString&lt;K,String&gt; operator template">
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
public class FirstTillMatchString<K> extends BaseMatchOperator<K,String>
{
  @InputPortFieldAnnotation(name="data")
  public final transient DefaultInputPort<HashMap<K, String>> data = new DefaultInputPort<HashMap<K, String>>()
  {
    /**
     * Compares the key,val pair with the match condition. Till a match is found tuples are emitted.
     * Once a match is found, state is set to emitted, and no more tuples are compared (no more emits).
     */
    @Override
    public void process(HashMap<K, String> tuple)
    {
      if (emitted) {
        return;
      }
      String val = tuple.get(getKey());
      double tvalue = 0;
      if (val == null) { // skip if key does not exist
        return;
      }
      boolean error = false;
      try {
        tvalue = Double.valueOf(val.toString()).doubleValue();
      }
      catch (NumberFormatException e) {
        error = true;
      }
      if (!error) {
        if (compareValue(tvalue)) {
          emitted = true;
        }
      }
      if (!emitted) {
        first.emit(cloneTuple(tuple));
      }
    }
  };

  @OutputPortFieldAnnotation(name="first")
  public final transient DefaultOutputPort<HashMap<K, String>> first = new DefaultOutputPort<HashMap<K, String>>();
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
