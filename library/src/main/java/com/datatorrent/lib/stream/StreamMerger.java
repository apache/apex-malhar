/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Merges two streams with identical schema and emits the tuples to the output port in order<p>
 * This is a pass through operator<br>
 * <b>Ports</b>:<br>
 * <b>data1</b>: expects K<br>
 * <b>data2</b>: expects K<br>
 * <b>out</b>: emits K<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for StreamMerger&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 500 Million tuples/s</td><td>Each in-bound tuple results in emit of 1 out-bound tuples</td><td>In-bound rate is the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for StreamMerger&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th colspan=2>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data1</i>(K)</th><th><i>data2</i>(K)</th><th><i>out</i>(K)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td><td>a</td></tr>
 * <tr><td>Data (process())</td><td></td><td>b</td><td>b</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class StreamMerger<K> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data1")
 public final transient DefaultInputPort<K> data1 = new DefaultInputPort<K>()
  {
    /**
     * Emits to port "out"
     */
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };

  @InputPortFieldAnnotation(name = "data2")
  public final transient DefaultInputPort<K> data2 = new DefaultInputPort<K>()
  {
    /**
     * Emits to port "out"
     */
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };

  @OutputPortFieldAnnotation(name = "out")
  public final transient DefaultOutputPort<K> out = new DefaultOutputPort<K>();

  /**
   * Allows usage of StreamMerger in a automated way
   *
   * @param i port index
   * @return returns the proper input port name
   */
  static public String getInputName(int i)
  {
    String ret = "illegal_portnumber";
    if ((i != 0) && (i <= getNumberOfInputPorts())) {
      ret = "data";
      ret += Integer.toString(i);
    }
    return ret;
  }

  static public int getNumberOfInputPorts()
  {
    return 2;
  }

  public DefaultInputPort<K> getInputPort(int i)
  {
    DefaultInputPort<K> ret = null;
    switch(i) {
      case 1:
        ret = data1;
        break;
      case 2:
        ret = data2;
        break;
      default:
        break;
    }
    return ret;
  }
}
