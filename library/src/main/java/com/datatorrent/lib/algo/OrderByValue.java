/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyValueOperator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.commons.lang.mutable.MutableInt;

/**
 * Order by ascending on value is done on the incoming stream based on key, and result is emitted on end of window<p>
 * This is an end of window module. At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>ordered_list</b>: emits HashMap&lt;K,V&gt;<br>
 * <b>ordered_count</b>: emits HashMap&lt;K,HashMap&lt;V,Integer&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:
 * filterBy: list of keys to filter (pass through)<br>
 * inverse: If set to true, all keys except filterBy pass through. Default value is false<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * If inverse is false, filterBy cannot be empty<br>
 * <br>
 * <b>Specific run time checks are</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for OrderByKey&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 6 Million K,V pairs/s</b></td><td>All tuples are emitted on port ordered_list, and one per value is emitted on port ordered_count</td>
 * <td>In-bound throughput and value diversity is the main determinant of performance.
 * The benchmark was done with immutable objects. If K or V are mutable the benchmark may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); orderBy=a</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for OrderByKey&lt;K,V&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(HashMap&lt;K,V&gt;)</th><th><i>ordered_count</i>(HashMap&lt;K,HashMap&lt;V,Integer&gt;(1)&gt;(1))</th><th><i>ordered_list</i>(HashMap&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><</tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5,c=6}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=50,c=16}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1,b=2,c=3}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1,b=7,c=4}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,b=23,c=33}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=5,c=6}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{e=2,b=5,c=6}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=-1,b=-50,c=6}</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{b={-50=1}}<br>{a={-1=1}}<br>{a={1=2}}<br>{b={2=1}}<br>{a={2=2}}<br>{a={3=1}}<br>{b={5=3}}<br>{b={7=1}}<br>{b={23=1}}<br>{b={50=1}}</td>
 * <td>{b=-50}<br>{a=-1}<br>{a=1}<br>{a=1}<br>{b=2}<br>{a=2}<br>{a=2}<br>{a=3}<br>{b=5}<br>{b=5}<br>{b=5}<br>{b=7}<br>{b=23}<br>{b=50}</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class OrderByValue<K, V> extends BaseKeyValueOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>()
  {
    /**
     * Processes each tuple, and orders by the given value
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        boolean fcontains = filterBy.containsKey(e.getKey());
        boolean skip = (inverse && fcontains) || (!inverse && !fcontains);
        if (skip) {
          continue;
        }

        HashMap<K, MutableInt> istr = smap.get(e.getValue());
        if (istr == null) { // not in priority queue
          istr = new HashMap<K, MutableInt>(4);
          smap.put(cloneValue(e.getValue()), istr);
          pqueue.add(cloneValue(e.getValue()));
        }
        MutableInt scount = istr.get(e.getKey());
        if (scount == null) { // this key does not exist
          scount = new MutableInt(0);
          istr.put(cloneKey(e.getKey()), scount);
        }
        scount.increment();
      }
    }
  };
  @OutputPortFieldAnnotation(name = "ordered_list")
  public final transient DefaultOutputPort<HashMap<K, V>> ordered_list = new DefaultOutputPort<HashMap<K, V>>();
  @OutputPortFieldAnnotation(name = "ordered_count")
  public final transient DefaultOutputPort<HashMap<K, HashMap<V, Integer>>> ordered_count = new DefaultOutputPort<HashMap<K, HashMap<V, Integer>>>();
  protected PriorityQueue<V> pqueue = null;
  protected HashMap<V, HashMap<K, MutableInt>> smap = new HashMap<V, HashMap<K, MutableInt>>();
  HashMap<K, Object> filterBy = new HashMap<K, Object>();
  boolean inverse = false;

  /**
   * setter function for filter
   *
   * @param list list of keys to filter
   */
  public void setFilterBy(K[] list)
  {
    if (list != null) {
      for (K s: list) {
        filterBy.put(s, null);
      }
    }
  }

  /**
   * getter function for inverse
   * @return the value of inverse
   */
  public boolean getInverse()
  {
    return inverse;
  }

  /**
   * Setter function for inverse. The filter is a negative filter is inverse is set to true
   * @param i value of inverse
   */
  public void setInverse(boolean i)
  {
    inverse = i;
  }

  /**
   * Initializes the priority queue in ascending order
   */
  public void initializePriorityQueue()
  {
    pqueue = new PriorityQueue<V>(5);
  }

  /**
   * Sets up the priority queue
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    initializePriorityQueue();
    int count = filterBy.size();
    if ((count == 0) && !inverse) {
      throw new IllegalArgumentException("filterBy not set");
    }
  }

  /**
   * Initializes queue once in the life of the operator
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    if (pqueue == null) {
      initializePriorityQueue();
    }
  }

  /**
   * Emits tuples
   */
  @Override
  public void endWindow()
  {
    V ival;
    while ((ival = pqueue.poll()) != null) {
      HashMap<K, MutableInt> istr = smap.get(ival);
      if (istr == null) { // Should never be null
        continue;
      }
      for (Map.Entry<K, MutableInt> e: istr.entrySet()) {
        final int count = e.getValue().intValue();
        if (ordered_list.isConnected()) {
          for (int i = 0; i < count; i++) {
            HashMap<K, V> tuple = new HashMap<K, V>(1);
            tuple.put(e.getKey(), ival);
            ordered_list.emit(tuple);
          }
        }
        if (ordered_count.isConnected()) {
          HashMap<K, HashMap<V, Integer>> tuple = new HashMap<K, HashMap<V, Integer>>(1);
          HashMap<V, Integer> odata = new HashMap<V, Integer>(1);
          odata.put(ival, new Integer(count));
          tuple.put(e.getKey(), odata);
          ordered_count.emit(tuple);
        }
      }
    }
    pqueue.clear();
    smap.clear();
  }
}
