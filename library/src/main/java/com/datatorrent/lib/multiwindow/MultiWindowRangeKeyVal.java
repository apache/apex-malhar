/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.multiwindow;

import com.datatorrent.lib.math.RangeKeyVal;
import com.datatorrent.lib.util.HighLow;
import com.datatorrent.lib.util.KeyValPair;

import java.util.Map;
import javax.validation.constraints.Min;
import org.apache.commons.lang.mutable.MutableDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A range operator of KeyValPair schema which calculate range across multiple streaming windows. <br>
 * This is an end window operator which emits only at Nth window. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>range</b>: emits KeyValPair&lt;K,HighLow&lt;V&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <b>windowSize i.e. N</b>: Number of streaming windows that define application window.<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Range&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>TBD Million K,V pairs/s</b></td><td>One K,ArrayList(2) pair per key per window</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Range&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(KeyValPair&lt;K,V&gt;)</th><th><i>range</i>(KeyValPair&lt;K,HighLow&lt;V&gt;&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=-1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=23}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>{a=[23,-1],b=[20,5],c=[1000,1000],d=[55,4],e=[2,2]</td></tr>
 * </table>
 * <br>
 *
 * <br>
 */
public class MultiWindowRangeKeyVal<K, V extends Number> extends RangeKeyVal<K, V>
{
  private static final Logger logger = LoggerFactory.getLogger(MultiWindowRangeKeyVal.class);

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
   * Emits range for each key at application window boundary. If no data is received, no emit is done
   * Clears the internal data before return
   */
  @Override
  public void endWindow()
  {
    boolean emit = (++windowCount) % windowSize == 0;

    if (!emit) {
      return;
    }

    for (Map.Entry<K, V> e: high.entrySet()) {
      HighLow hl = new HighLow();
      hl.setHigh(getValue(e.getValue().doubleValue()));
      hl.setLow(getValue(low.get(e.getKey()).doubleValue())); // cannot be null

      range.emit(new KeyValPair(e.getKey(), hl));
    }
    clearCache();
  }
}

