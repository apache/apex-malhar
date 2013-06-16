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
import com.datatorrent.lib.util.UnifierBooleanAnd;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Each tuple is tested for the compare function. The function is given by
 * "key", "value", and "cmp". If all tuples passes a Boolean(true) is emitted, else a Boolean(false) is emitted on end of window on the output port "all".
 * The comparison is done by getting double value from the Number.<p>
 * This module is an end of window module<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,String&gt;<br>
 * <b>all</b>: emits Boolean<br>
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
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for MatchAllStringMap&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>12 to 500 Million K,String pairs/s depending on all match</b></td><td>One Boolean per window</td>
 * <td>In-bound throughput is the main determinant of performance. Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String); key=a; value=3; cmp=eq</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for MatchAllStringMap&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,String&gt;)</th><th><i>all</i>(Boolean)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=5,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,h=20,c=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2,a=3}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=1,d=5,d=4}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=23}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>false</td></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=3,b=20,c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,h=20,c=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2,a=3}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>true</td></tr>
 * </table>
 * <br>
 *
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class MatchAllStringMap<K> extends BaseMatchOperator<K, String>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, String>> data = new DefaultInputPort<Map<K, String>>(this)
  {
    /**
     * Sets match flag to false for on first non matching tuple
     */
    @Override
    public void process(Map<K, String> tuple)
    {
      if (!result) {
        return;
      }
      String val = tuple.get(getKey());
      if (val == null) { // skip if key does not exist
        return;
      }
      double tvalue = 0;
      boolean errortuple = false;
      try {
        tvalue = Double.parseDouble(val.toString());
      }
      catch (NumberFormatException e) {
        errortuple = true;
      }
      result = !errortuple
              && compareValue(tvalue);
    }
  };
  @OutputPortFieldAnnotation(name = "all")
  public final transient DefaultOutputPort<Boolean> all = new DefaultOutputPort<Boolean>(this)
  {
    @Override
    public Unifier<Boolean> getUnifier()
    {
      return new UnifierBooleanAnd();
    }
  };
  protected boolean result = true;

  /**
   * Emits the match flag
   */
  @Override
  public void endWindow()
  {
    all.emit(result);
    result = true;
  }
}
