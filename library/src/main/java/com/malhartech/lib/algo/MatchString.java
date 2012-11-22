/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseMatchOperator;
import java.util.HashMap;

/**
 *
 * A compare function is imposed based on the property "key", "value", and "cmp". If the tuple
 * passed the test, it is emitted on the output port "match". The comparison is done by getting double
 * value from the Number. Both output ports are optional, but at least one has to be connected<p>
 * This module is a pass through<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,String&gt;<br>
 * <b>match</b>: emits HashMap&lt;K,String&gt; if compare function returns true<br>
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
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Match&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>12 Million K,String pairs/s (about 4 Million K,V pairs/s emitted</b></td><td>All tuples that match are emitted</td>
 * <td>In-bound throughput and number of matching tuples are the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); key=a; value=3; cmp=eq</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Match&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,String&gt;)</th><th><i>match</i>(HashMap&lt;K,String&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=5,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,h=20,c=2}</td><td>{a=3,h=20,c=2}</td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2,a=3}</td><td>{d=46,e=2,a=3}</td></tr>
 * <tr><td>Data (process())</td><td>{d=1,d=5,d=4}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=23}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class MatchString<K, String> extends BaseMatchOperator<K,String>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, String>> data = new DefaultInputPort<HashMap<K, String>>(this)
  {
    /**
     * Matchs tuple with the value and calls tupleMatched and tupleNotMatched based on if value matches
     */
    @Override
    public void process(HashMap<K, String> tuple)
    {
      String val = tuple.get(getKey());
      if (val == null) { // skip this tuple
        if (emitError) {
          tupleNotMatched(tuple);
        }
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
      if (!errortuple) {
        if (compareValue(tvalue)) {
          tupleMatched(tuple);
        }
        else {
          tupleNotMatched(tuple);
        }
      }
      else if (emitError) {
        tupleNotMatched(tuple);
      }
    }
  };

  @OutputPortFieldAnnotation(name = "match", optional=true)
  public final transient DefaultOutputPort<HashMap<K, String>> match = new DefaultOutputPort<HashMap<K, String>>(this);

  boolean emitError = true;

  /**
   * getter function for emitError flag.<br>
   * Error tuples (no key; val not a number) are emitted if this flag is true. If false they are simply dropped
   * @return emitError
   */
  public boolean getEmitError()
  {
    return emitError;
  }

  /**
   * setter funtion for emitError flag
   * @param val
   */
  public void setEmitError(boolean val)
  {
    emitError = val;
  }

  /**
   * Emits tuple if it. Call cloneTuple to allow users who have mutable objects to make a copy
   * @param tuple
   */
  public void tupleMatched(HashMap<K, String> tuple)
  {
    match.emit(cloneTuple(tuple));
  }

  /**
   * Does not emit tuple, an empty call. Sub class can override
   * @param tuple
   */
  public void tupleNotMatched(HashMap<K, String> tuple)
  {
  }
}
