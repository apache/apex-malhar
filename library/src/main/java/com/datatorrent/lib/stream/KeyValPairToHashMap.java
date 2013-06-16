/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.util.BaseKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Takes a KeyValPair and emits a HashMap(1), Used for for converting KeyValPair to a HashMap(1) tuple<p>
 * This is a pass through operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>keyval</b>: expects KeyValPair&lt;K,V&gt;<br>
 * <b>map</b>: emits HashMap&lt;K,V&gt;<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for KeyValPairToHashMap&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 20 Million tuples/s</td><td>Each in-bound tuple results in one out-bound tuple</td><td>In-bound rate is the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for KeyValPairToHashMap&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>keyval</i>(KeyValPair&lt;K,V&gt;)</th><th><i>map</i>(HashMap&lt;K,V&gt;(1))</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2}</td><td>{a=2}</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */

public class KeyValPairToHashMap<K, V> extends BaseKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K, V>> keyval = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    /**
     * Emits key, key/val pair, and val based on port connections
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      HashMap<K,V> otuple = new HashMap<K,V>(1);
      otuple.put(tuple.getKey(), tuple.getValue());
      map.emit(otuple);
    }
  };
  @OutputPortFieldAnnotation(name = "map")
  public final transient DefaultOutputPort<HashMap<K, V>> map = new DefaultOutputPort<HashMap<K, V>>(this);
}
