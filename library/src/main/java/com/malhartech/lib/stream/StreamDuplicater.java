/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyOperator;

/**
 * Duplicates an input stream as is into two output streams; needed to allow separation of listeners into two streams with different properties (for example
 * inline vs in-rack)<p>
 * This is a pass through operator<br>
 * <br>
 * <b>Port Interface</b><br>
 * <b>data</b>: expects &lt;K&gt;<br>
 * <b>out1</b>: emits &lt;K&gt;<br>
 * <b>out2</b>: emits &lt;K&gt;<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for StreamDuplicater&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 500 Million tuples/s</td><td>Each in-bound tuple results in emit of 2 out-bound tuples</td><td>In-bound rate is the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for StreamDuplicater&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(K)</th><th><i>out1</i>(K)</th><th><i>out1</i>(K)</th>/tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>a</td><td>a</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>b</td><td>b</td><td>b</td></tr>
 * <tr><td>Data (process())</td><td>c</td><td>c</td><td>c</td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */

public class StreamDuplicater<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    /**
     * Emits tuple on both streams
     */
    @Override
    public void process(K tuple)
    {
      out1.emit(cloneKey(tuple));
      out2.emit(cloneKey(tuple));
    }
  };

  @OutputPortFieldAnnotation(name = "out1")
  public final transient DefaultOutputPort<K> out1 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out2")
  public final transient DefaultOutputPort<K> out2 = new DefaultOutputPort<K>(this);
  final transient static int num_oport = 2;

  /**
   * Allows usage of StreamDuplicater in a automated way
   *
   * @param i port index
   * @return returns the proper input port name
   */
  static public String getOutputName(int i)
  {
    String ret = "illegal_portnumber";
    if ((i != 0) && (i <= num_oport)) {
      ret = "out";
      ret += Integer.toString(i);
    }
    return ret;
  }

  /**
   * returns number of output ports on this operator
   */
  public int getNumOutputPorts()
  {
    return num_oport;
  }
}
