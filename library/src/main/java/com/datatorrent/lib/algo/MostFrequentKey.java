/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.AbstractBaseFrequentKey;
import com.malhartech.lib.util.UnifierHashMapFrequent;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * Occurrences of each tuple is counted and at the end of window any of the most frequent tuple is emitted on output port least and all least frequent
 * tuples on output port list<p>
 * This module is an end of window module<br>
 * In case of a tie any of the least key would be emitted. The list port would however have all the tied keys
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects K<br>
 * <b>most</b>: emits HashMap&lt;K,Integer&gt;(1), Where K is the least occurring key in the window. In case of tie any of the least key would be emitted<br>
 * <b>list</b>: emits ArrayList&lt;HashMap&lt;K,Integer&gt;(1)&gt, Where the list includes all the keys that are least frequent<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for LeastFrequentKey&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 60 Million tuple/s</b></td><td>Emits only 1 tuple per window per port</td><td>In-bound throughput is the main determinant of performance.
 * The benchmark was done with immutable K. If K is mutable the benchmark may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String);</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for LeastFrequentKey&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(K)</th><th><i>least</i>(HashMap&lt;K,Integer&gt;)</th><th><i>list</i>(ArrayList&kt;HashMap&lt;K,Integer&gt;&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>d</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>c</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>h</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>b</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>f</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>c</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>b</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>b</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>z</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{a=9}</td><td>[{a=9}]</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class MostFrequentKey<K> extends AbstractBaseFrequentKey<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    /**
     * Calls super.processTuple(tuple)
     */
    @Override
    public void process(K tuple)
    {
      processTuple(tuple);
    }
  };
  @OutputPortFieldAnnotation(name = "most")
  public final transient DefaultOutputPort<HashMap<K, Integer>> most = new DefaultOutputPort<HashMap<K, Integer>>(this)
  {
    @Override
    public Unifier<HashMap<K, Integer>> getUnifier()
    {
      UnifierHashMapFrequent ret = new UnifierHashMapFrequent<K>();
      ret.setLeast(false);
      return ret;
    }
  };


  @OutputPortFieldAnnotation(name = "list")
  public final transient DefaultOutputPort<ArrayList<HashMap<K, Integer>>> list = new DefaultOutputPort<ArrayList<HashMap<K, Integer>>>(this);

  /**
   * Emits tuple on port "most"
   * @param tuple
   */
  @Override
  public void emitTuple(HashMap<K, Integer> tuple)
  {
    most.emit(tuple);
  }

  /**
   * Emits tuple on port "list"
   * @param tlist
   */
  @Override
  public void emitList(ArrayList<HashMap<K, Integer>> tlist)
  {
    list.emit(tlist);
  }

  /**
   * returns val1 < val2
   * @param val1
   * @param val2
   * @return val1 > val2
   */
  @Override
  public boolean compareCount(int val1, int val2)
  {
    return val1 > val2;
  }
}
