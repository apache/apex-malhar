/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyValueOperator;
import com.malhartech.lib.util.MutableInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import javax.validation.constraints.NotNull;

/**
 * Order by ascending is done on the incoming stream based on val, and result is emitted on end of window<p>
 * This is an end of window module. At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>ordered_count</b>: emits HashMap&lt;V,Integer&gt;<br>
 * <b>ordered_list</b>: emits HashMap&lt;K,V&gt;<br>
 * <br>
 * <b>Properties</b>:
 * <b>orderby</b>: The key to order by<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * orderBy cannot be empty<br>
 * <br>
 * <b>Specific run time checks are</b>:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for OrderByKey&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 5 Million K,V pairs/s</b></td><td>All tuples are emitted on port ordered_list, and one per value is emitted on port ordered_count</td>
 * <td>In-bound throughput and value diversity is the main determinant of performance.
 * The benchmark was done with immutable objects. If K or V are mutable the benchmark may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); orderBy=a</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for OrderByKey&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>ordered_count</i>(HashMap&lt;V,Integer&gt;)</th><th><i>ordered_list</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><</tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5,c=6}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=50,c=16}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5,c=6}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1,b=2,c=3}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1,b=7,c=4}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,b=23,c=33}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5,c=6}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{e=2,b=5,c=6}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=-1,b=-5,c=6}</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{-1=1}<br>{1=2}<br>{2=2}<br>{3=1}</td>
 * <td>{b=-5, c=6, a=-1}<br>{b=2, c=3, a=1}<br>{b=7, c=4, a=1}<br>{b=5, c=6, a=2}<br>{b=5, c=6, a=2}<br>{b=23, c=33, a=3}<br></td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class OrderByKey<K,V> extends BaseKeyValueOperator<K,V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>(this)
  {
    /**
     * process the tuple is orderby val exists.
     */
    @Override
    public void process(HashMap<K,V> tuple)
    {
      V val = tuple.get(orderby);
      if (val == null) {
        // emit error tuple?
        return;
      }
      boolean first = false;
      if (ordered_count.isConnected()) {
        MutableInteger count = countmap.get(val);
        if (count == null) {
          count = new MutableInteger(0);
          countmap.put(cloneValue(val), count);
          first = true;
        }
        count.value++;
      }
      if (ordered_list.isConnected()) {
        ArrayList<HashMap<K,V>> list = smap.get(val);
        if (list == null) {
          list = new ArrayList<HashMap<K,V>>();
          smap.put(cloneValue(val), list);
          first = true;
        }
        list.add(cloneTuple(tuple));
      }
      if (first) {
        pqueue.add(cloneValue(val));
      }
    }
  };

  @OutputPortFieldAnnotation(name = "ordered_list")
  public final transient DefaultOutputPort<HashMap<K,V>> ordered_list = new DefaultOutputPort<HashMap<K,V>>(this);
  @OutputPortFieldAnnotation(name = "ordered_count")
  public final transient DefaultOutputPort<HashMap<V,Integer>> ordered_count = new DefaultOutputPort<HashMap<V,Integer>>(this);

  @NotNull()
  K orderby = null;
  protected PriorityQueue<V> pqueue = null;
  protected HashMap<V,MutableInteger> countmap = new HashMap<V,MutableInteger>();
  protected HashMap<V,ArrayList<HashMap<K,V>>> smap = new HashMap<V,ArrayList<HashMap<K,V>>>();

  /**
   * getter function for orderby
   * @return orderby
   */
  @NotNull()
  public K getOrderby()
  {
    return orderby;
  }

  /**
   * setter function for orderby val
   * @param str
   */
  public void setOrderby(K str)
  {
    orderby = str;
  }

  /**
   * First cut of the priority queue in ascending order
   */
  public void initializePriorityQueue() {
    pqueue = new PriorityQueue<V>(5);
  }

  /**
   * Sets up the priority queue
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    if (getOrderby() == null) {
      throw new IllegalArgumentException("Orderby key not set");
    }
    initializePriorityQueue();
  }

  /**
   * Cleanup at the start of window
   */
  @Override
  public void beginWindow(long windowId)
  {
    if (pqueue == null) {
      initializePriorityQueue();
    }
    pqueue.clear();
    countmap.clear();
    smap.clear();
  }

  /**
   * Emits tuples
   */
  @Override
  public void endWindow()
  {
    V val;
    while ((val = pqueue.poll()) != null) {
      if (ordered_count.isConnected()) {
        HashMap<V, Integer> tuple = new HashMap<V, Integer>(1);
        tuple.put(val, countmap.get(val).value);
        ordered_count.emit(tuple);
      }
      if (ordered_list.isConnected()) {
        ArrayList<HashMap<K, V>> list = smap.get(val);
        for (HashMap<K, V> o: list) {
          ordered_list.emit(o);
        }
      }
    }
  }
}
