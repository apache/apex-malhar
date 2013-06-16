/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.AbstractBaseSortOperator;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Takes a stream of key value pairs via input port "data". The incoming tuple is merged into already existing sorted list.
 * At the end of the window the entire sorted list is emitted on output port "sort"<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects K<br>
 * <b>datalist</b>: expects ArrayList&lt;K&gt;<br>
 * <b>sortlist</b>: emits ArrayList&lt;K&gt;<br>
 * <b>sorthash</b>: emits HashMap&lt;K,Integer&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks are</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for InsertSort&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 2.5 Million tuples/s on average</b></td><td>All tuples inserted one at a time</td>
 * <td>In-bound throughput (i.e. total number of tuples in the window) is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for InsertSort&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th colspan=2>In-bound (process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(K)</th><th><i>datalsit</i>(ArrayList&lt;K&gt;)</th><th><i>sortlist</i>(ArrayList&lt;K&gt;)</th><th><i>sorthash</i>(HashMap&lt;K,Integer&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>-4</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>20</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>[-5,-4,-10]</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>3</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>15</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>100</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>-10</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>1</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>3</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>[1,1,2]</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>15</td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>20</td><td></td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>[-10,-10,-5,-4,-4,1,1,1,2,2,2,3,3,15,15,20,20,100]</td>
 * <td>{-10=2,-5=1,-4=2,1=3,2=3,3=2,15=2,20=2,100=1}</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
//
// TODO: Override PriorityQueue and rewrite addAll to insert with location
//
public class InsertSort<K> extends AbstractBaseSortOperator<K>
{
  /**
   * Input port that takes in one tuple at a time
   */
  @InputPortFieldAnnotation(name = "data", optional = true)
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    /**
     * Adds tuple to sorted queue
     */
    @Override
    public void process(K tuple)
    {
      processTuple(tuple);
    }
  };
  /**
   * Input port that takes in an array of Objects to insert
   */
  @InputPortFieldAnnotation(name = "datalist", optional = true)
  public final transient DefaultInputPort<ArrayList<K>> datalist = new DefaultInputPort<ArrayList<K>>(this)
  {
    /**
     * Adds tuples to sorted queue
     */
    @Override
    public void process(ArrayList<K> tuple)
    {
      processTuple(tuple);
    }
  };

  @OutputPortFieldAnnotation(name = "sort", optional = true)
  public final transient DefaultOutputPort<ArrayList<K>> sort = new DefaultOutputPort<ArrayList<K>>(this);
  @OutputPortFieldAnnotation(name = "sorthash", optional = true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> sorthash = new DefaultOutputPort<HashMap<K, Integer>>(this);



  @Override
  public void emitToList(ArrayList<K> list)
  {
    sort.emit(list);
  }

  @Override
  public void emitToHash(HashMap<K,Integer> map)
  {
    sorthash.emit(map);
  }

  @Override
  public boolean doEmitList()
  {
    return sort.isConnected();
  }

  @Override
  public boolean doEmitHash()
  {
    return sorthash.isConnected();
  }
}
