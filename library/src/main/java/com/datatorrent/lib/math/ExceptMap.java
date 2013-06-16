/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.lib.algo.MatchMap;
import com.datatorrent.lib.util.UnifierHashMap;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * A compare operation is done on tuple sub-classed from Number based on the property "key", "value", and "cmp", and not matched tuples are emitted.
 * The comparison is done by getting double value from the Number. Both output ports are optional, but at least one has to be connected<p>
 * This module is a pass through<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>except</b>: emits HashMap&lt;K,V&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp</b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * <b>Compile time checks</b>:<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * <b>Run time checks</b>:<br>
 * Does the incoming HashMap have the key<br>
 * Is the value of the key a number<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for ExceptMap&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 10 Million K,V pairs/s</b></td><td>Each not matched tuple is emitted</td><td>In-bound rate and number of tuples that do not match determine performance.
 * Immutable tuples were used in the benchmarking. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); emitError=true; key=a; value=3; cmp=eq)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for ExceptMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(Map&lt;K,V&gt;)</th><th><i>except</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td>{a=2,b=20,c=1000}</td></tr>
 * <tr><td>Data (process())</td><td>{a=3,b=40,c=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td>{a=10,b=5}</td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td>{d=55,b=12}</td></tr>
 * <tr><td>Data (process())</td><td>{d=22,a=4}</td><td>{d=22,a=4}</td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=3,g=5,h=44}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class ExceptMap<K, V extends Number> extends MatchMap<K, V>
{
  @OutputPortFieldAnnotation(name = "except")
  public final transient DefaultOutputPort<HashMap<K, V>> except = new DefaultOutputPort<HashMap<K, V>>(this)
  {
    @Override
    public Unifier<HashMap<K, V>> getUnifier()
    {
      return new UnifierHashMap<K, V>();
    }
  };

  /**
   * Does nothing. Overrides base as call super.tupleMatched() would emit the tuple
   * @param tuple
   */
  @Override
  public void tupleMatched(Map<K, V> tuple)
  {
  }

  /**
   * Emits the tuple. Calls cloneTuple to get a copy, allowing users to override in case objects are mutable
   * @param tuple
   */
  @Override
  public void tupleNotMatched(Map<K, V> tuple)
  {
    except.emit(cloneTuple(tuple));
  }
}
