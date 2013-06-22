/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.AbstractBaseFrequentKey;
import com.datatorrent.lib.util.UnifierHashMapFrequent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Occurrences of each key is counted and at the end of window any of the least frequent key is emitted on output port least and all least frequent
 * keys on output port list<p>
 * This module is an end of window module. In case of a tie any of the least key would be emitted. The list port would however have all the tied keys<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V&gt;, V is ignored/not used<br>
 * <b>least</b>: emits HashMap&lt;K,Integer&gt;(1); where String is the least frequent key, and Integer is the number of its occurrences in the window<br>
 * <b>list</b>: emits ArrayList&lt;HashMap&lt;K,Integer&gt;(1)&gt;; Where the list includes all the keys are least frequent<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for LeastFrequentKeyMap&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 30 Million K,V pairs/s</b></td><td>Emits only 1 tuple per window per port</td><td>In-bound throughput is the main determinant of performance.
 * The benchmark was done with immutable K. If K is mutable the benchmark may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer);</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for LeastFrequentKeyMap&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(Map&lt;K,V&gt;)</th><th><i>least</i>(HashMap&lt;K,Integer&gt;)</th><th><i>list</i>(ArrayList&kt;HashMap&lt;K,Integer&gt;&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=1,b=5,c=110}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=-3,c=2000,i=55,j=45}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=2}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=55,b=5,c=22,n=45,m=33}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{h=20,f=45}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=4,c=4}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=19,z=2}</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{z=1}</td><td>[{d=1},{z=1},{h=1},{f=1}]</td></tr>
 * </table>
 * <br>
 *
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class LeastFrequentKeyMap<K, V> extends AbstractBaseFrequentKey<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Map<K, V>> data = new DefaultInputPort<Map<K, V>>()
  {
    /**
     * Calls super.processTuple(tuple) for each key in the HashMap
     */
    @Override
    public void process(Map<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        processTuple(e.getKey());
      }
    }
  };
  @OutputPortFieldAnnotation(name = "least", optional = true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> least = new DefaultOutputPort<HashMap<K, Integer>>()
  {
    @Override
    public Unifier<HashMap<K, Integer>> getUnifier()
    {
      UnifierHashMapFrequent ret = new UnifierHashMapFrequent<K>();
      ret.setLeast(true);
      return ret;
    }
  };
  @OutputPortFieldAnnotation(name = "list", optional = true)
  public final transient DefaultOutputPort<ArrayList<HashMap<K, Integer>>> list = new DefaultOutputPort<ArrayList<HashMap<K, Integer>>>();

  /**
   * Emits tuple on port "least"
   *
   * @param tuple
   */
  @Override
  public void emitTuple(HashMap<K, Integer> tuple)
  {
    least.emit(tuple);
  }

  /**
   * Emits tuple on port "list"
   *
   * @param tlist
   */
  @Override
  public void emitList(ArrayList<HashMap<K, Integer>> tlist)
  {
    list.emit(tlist);
  }

  /**
   * returns val1 < val2
   *
   * @param val1
   * @param val2
   * @return val1 < val2
   */
  @Override
  public boolean compareCount(int val1, int val2)
  {
    return val1 < val2;
  }
}
