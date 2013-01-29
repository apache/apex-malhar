/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.multiwindow;

import com.malhartech.lib.math.SumKeyVal;
import com.malhartech.lib.util.KeyValPair;
import com.malhartech.lib.util.MutableDouble;
import com.malhartech.lib.util.MutableInteger;
import java.util.Map;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sum operator of KeyValPair schema which accumulates sum across multiple streaming windows. <p>
 * This is an end window operator which emits only at Nth window. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>sum</b>: emits KeyValPair&lt;K,V&gt;<br>
 * <b>count</b>: emits KeyValPair&lt;K,Integer&gt;</b><br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: If set to true the key in the filter will block tuple.<br>
 * <b>filterBy</b>: List of keys to filter on.<br>
 * <b>windowSize i.e. N</b>: Number of streaming windows that define application window.<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>TBD million tuples/s</b></td><td>One tuple per key per port</td><td>Mainly dependant on in-bound throughput</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(KeyValPair&lt;K,V&gt;)</th><th><i>sum</i>(KeyValPair&lt;K,V&gt;)</th><th><i>count</i>(KeyValPair&lt;K,Integer&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=20}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{c=1000}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=5}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=12}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{e=2}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=23}</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td>
 * <td>{a=36}<br>{b=37}<br>{c=1000}<br>{d=141}<br>{e=2}</td>
 * <td>{a=4}<br>{b=3}<br>{c=1}<br>{d=5}<br>{e=1}</td></tr>
 * </table>
 * <br>
 *
 * @author Locknath Shil <locknath@malhar-inc.com><br>
 * <br>
 */
public class MultiWindowSumKeyVal<K, V extends Number> extends SumKeyVal<K, V>
{
  private static final Logger logger = LoggerFactory.getLogger(MultiWindowSumKeyVal.class);

  /**
   * Number of streaming window after which tuple got emitted.
   */
  @Min(2)
  private int windowSize = 2;
  private long windowCount = 0;

  public void setWindowSize(int windowSize)
  {
    this.windowSize = windowSize;
  }

  /**
   * Emit only at the end of windowSize window boundary.
   */
  @Override
  public void endWindow()
  {
    boolean emit = (++windowCount) % windowSize == 0;

    if (!emit) {
      return;
    }

    // Emit only at the end of application window boundary.
    boolean dosum = sum.isConnected();

    if (dosum) {
      for (Map.Entry<K, MutableDouble> e: sums.entrySet()) {
        K key = e.getKey();
        if (dosum) {
          sum.emit(new KeyValPair(key, getValue(e.getValue().value)));
        }
      }
    }

    // Clear cumulative sum at the end of application window boundary.
    sums.clear();
  }
}

