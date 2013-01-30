/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

/**
 *
 * Emits the cumulative sum of values for each key at the end of window. <p>
 * This is an end window operator. Default unifier works as this operator follows sticky partition.<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>sum</b>: emits KeyValPair&lt;K,V extends Number&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: If set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>20 million tuples/s</b></td><td>One tuple per key per port</td><td>Mainly dependant on in-bound throughput</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(KeyValPair&lt;K,V&gt;)</th><th><i>sum</i>(KeyValPair&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=20}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{e=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=23}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td>
 * <td>{a=36}<br>{b=37}<br>{c=1000}<br>{d=141}<br>{e=2}</td> * </tr>
 * </table>
 * <br>
 *
 * @author Locknath Shil (locknath@malhar-inc.com)<br>
 * <br>
 */
public class CumulativeSumKeyVal<K, V extends Number> extends SumKeyVal<K, V>
{

  @Override
  public void clearCache()
  {
   // don't clear cache
  }
}
