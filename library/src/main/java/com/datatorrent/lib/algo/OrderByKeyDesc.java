/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import java.util.PriorityQueue;

/**
 * Order by descending is done on the incoming stream based on key, and result is emitted on end of window<p>
 * This is an end of window module. At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
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
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{3=1}<br>{2=2}<br>{1=2}<br>{-1=1}</td>
 * <td>{b=23, c=33, a=3}<br>{b=5, c=6, a=2}<br>{b=5, c=6, a=2}<br>{b=7, c=4, a=1}<br>{b=2, c=3, a=1}<br>{b=-5, c=6, a=-1}</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class OrderByKeyDesc<K,V> extends OrderByKey<K,V>
{
  /**
   * Initializes descending priority queue
   */
  @Override
  public void initializePriorityQueue()
  {
    pqueue = new PriorityQueue<V>(5, new ReversibleComparator<V>(false));
  }
}
