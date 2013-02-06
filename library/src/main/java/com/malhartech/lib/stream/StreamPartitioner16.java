/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyOperator;
import javax.validation.constraints.Min;

/**
 * Partitions the incoming stream into 12 streams. A hardcoded partitioner<p>
 * This is a pass through operator<br>
 * <br>
 * <b>Port Interface</b><br>
 * <b>data</b>: expects &lt;K&gt;<br>
 * <b>out1</b>: emits &lt;K&gt;<br>
 * <b>out2</b>: emits &lt;K&gt;<br>
 * <b>out3</b>: emits &lt;K&gt;<br>
 * <b>out4</b>: emits &lt;K&gt;<br>
 * <b>out5</b>: emits &lt;K&gt;<br>
 * <b>out6</b>: emits &lt;K&gt;<br>
 * <b>out7</b>: emits &lt;K&gt;<br>
 * <b>out8</b>: emits &lt;K&gt;<br>
 * <b>out9</b>: emits &lt;K&gt;<br>
 * <b>out10</b>: emits &lt;K&gt;<br>
 * <b>out11</b>: emits &lt;K&gt;<br>
 * <b>out12</b>: emits &lt;K&gt;<br>
 * <b>out13</b>: emits &lt;K&gt;<br>
 * <b>out14</b>: emits &lt;K&gt;<br>
 * <b>out15</b>: emits &lt;K&gt;<br>
 * <b>out16</b>: emits &lt;K&gt;<br>
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

public class StreamPartitioner16<K> extends BaseKeyOperator<K>
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
      int i = getPartition(tuple) / 16;
      switch (i) {
        case 1:
          out1.emit(cloneKey(tuple));
          break;
        case 2:
          out2.emit(cloneKey(tuple));
          break;
        case 3:
          out3.emit(cloneKey(tuple));
          break;
        case 4:
          out4.emit(cloneKey(tuple));
          break;
        case 5:
          out5.emit(cloneKey(tuple));
          break;
        case 6:
          out6.emit(cloneKey(tuple));
          break;
        case 7:
          out7.emit(cloneKey(tuple));
          break;
        case 8:
          out8.emit(cloneKey(tuple));
          break;
        case 9:
          out9.emit(cloneKey(tuple));
          break;
        case 10:
          out10.emit(cloneKey(tuple));
          break;
        case 11:
          out11.emit(cloneKey(tuple));
          break;
        case 12:
          out12.emit(cloneKey(tuple));
          break;
        case 13:
          out13.emit(cloneKey(tuple));
          break;
        case 14:
          out14.emit(cloneKey(tuple));
          break;
        case 15:
          out15.emit(cloneKey(tuple));
          break;
        case 16:
          out16.emit(cloneKey(tuple));
          break;
        default:
          out16.emit(cloneKey(tuple));
          break;
      }
    }
  };

  @OutputPortFieldAnnotation(name = "out1")
  public final transient DefaultOutputPort<K> out1 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out2")
  public final transient DefaultOutputPort<K> out2 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out3")
  public final transient DefaultOutputPort<K> out3 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out4")
  public final transient DefaultOutputPort<K> out4 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out5")
  public final transient DefaultOutputPort<K> out5 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out6")
  public final transient DefaultOutputPort<K> out6 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out7")
  public final transient DefaultOutputPort<K> out7 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out8")
  public final transient DefaultOutputPort<K> out8 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out9")
  public final transient DefaultOutputPort<K> out9 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out10")
  public final transient DefaultOutputPort<K> out10 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out11")
  public final transient DefaultOutputPort<K> out11 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out12")
  public final transient DefaultOutputPort<K> out12 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out13")
  public final transient DefaultOutputPort<K> out13 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out14")
  public final transient DefaultOutputPort<K> out14 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out15")
  public final transient DefaultOutputPort<K> out15 = new DefaultOutputPort<K>(this);
  @OutputPortFieldAnnotation(name = "out16")
  public final transient DefaultOutputPort<K> out16 = new DefaultOutputPort<K>(this);


  final transient static int num_oport = 16;
  @Min(2)
  public int numpartitions = 16;

  public int getPartition(K tuple)
  {
    return 0;
  }

  public int getNumpartiions()
  {
    return numpartitions;
  }

  public void setNumpartitions(int i)
  {
    numpartitions = i;
  }

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
