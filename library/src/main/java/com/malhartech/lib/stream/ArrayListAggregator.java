/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * Creates a ArrayList tuple from incoming tuples. The size of the ArrayList before it is emitted is determined by property \"size\".
 * This is a stateful operator by default as it waits for the ArrayList to be complete. The operator can be made stateless by setting
 * property \"flustWindow\" true. At the endWindow call the ArrayList is emitted, but the size of this tuple (ArrayList) is &lt;= to
 * the value of property \"size\".<p>
 * This operator can be used in many variations<br>
 * 1. Emit all the tuples in that window as one ArrayList:
 *
 * <br>
 * <b>Port</b>:<br>
 * <b>input</b>: expects T<br>
 * <b>output</b>: emits ArrayList&lt;T&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>size</b>: The size of ArrayList to be emitted of property passThrough is true. Default value is 1</b>
 * <b>flushWindow</b>: If true, flushes the ArrayList in endWindow no matter what size, and makes the operator stateless. Default value is false (stateful)</b>
 * <b>passThrough</b>: If true emits ArrayList in process() when ArrayList grows to size. If false ArrayList is emitted in endWindow. Default value is true.</b>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for DevNull operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 60 million tuples/s</td><td>One tuple (ArrayList) emitted for N (N=3) incoming tuples, where N is the number of keys</td>
 * <td>In-bound rate is the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (T=Integer), size = 3, passThrough = true, flushWindow=false</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for DevNull operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>No Outbound port</th></tr>
 * <tr><th><i>input</i>(V)</th><th><i>output</i>(HashMap&gt;K,V&lt;</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>66</td><td></td></tr>
 * <tr><td>Data (process())</td><td>5</td><td>[2,66,5]</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>-1</td><td></td></tr>
 * <tr><td>Data (process())</td><td>3</td><td>[2,-1,3]</td></tr>
 * <tr><td>Data (process())</td><td>12</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>13</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>5</td><td>[12,13,5]</td></tr>
 * <tr><td>Data (process())</td><td>21</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td></td><td></td></tr>
 * <tr><td>Begin Window (beginWindow())</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>4</td><td></td></tr>
 * <tr><td>Data (process())</td><td>5</td><td>[21,4,5]</td></tr>
 * <tr><td>End Window (endWindow())</td><td></td><td></td></tr>
 * </table>
 * <br>
 * 
 * @param <T> Type of elements in the collection.<br>
 * @author Chetan Narsude <chetan@malhar-inc.com><br>
 *
 */
public class ArrayListAggregator<T> extends AbstractAggregator<T>
{
  @Override
  public Collection<T> getNewCollection(int size)
  {
    return new ArrayList<T>(size);
  }

}
