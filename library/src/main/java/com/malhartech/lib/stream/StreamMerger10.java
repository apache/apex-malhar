/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.DefaultInputPort;

/**
 * Merges ten streams with identical schema and emits tuples on to the output port in order.<br>
 * The aim is to simply getUnifier two streams of same schema type<p>
 * <br>
 * <br>
 * Benchmarks: This node has been benchmarked at over 18 million tuples/second in local/inline mode<br>
 *
 * <b>Tuple Schema</b>: All tuples were treated as Object
 * <b>Port Interface</b><br>
 * <b>out</b>: Output port for emitting tuples<br>
 * <b>data1</b>: Input port for receiving the 1st stream of incoming tuple<br>
 * <b>data2</b>: Input port for receiving the 2nd stream of incoming tuple<br>
 * <b>data3</b>: Input port for receiving the 3rd stream of incoming tuple<br>
 * <b>data4</b>: Input port for receiving the 4th stream of incoming tuple<br>
 * <b>data5</b>: Input port for receiving the 5th stream of incoming tuple<br>
 * <b>data6</b>: Input port for receiving the 6th stream of incoming tuple<br>
 * <b>data7</b>: Input port for receiving the 7th stream of incoming tuple<br>
 * <b>data8</b>: Input port for receiving the 8th stream of incoming tuple<br>
 * <b>data9</b>: Input port for receiving the 9th stream of incoming tuple<br>
 * <b>data10</b>: Input port for receiving the 10th stream of incoming tuple<br>
 * <br>
 * <b>Properties</b>:
 * None
 * <br>
 * Compile time checks are:<br>
 * no checks are done. Schema check is compile/instantiation time. Not runtime<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator does >400 million tuples/sec as all tuples simply forwarded as is<br>
 * <br>
 *
 * @author amol
 */
public class StreamMerger10<K> extends StreamMerger5<K>
{
  public final transient DefaultInputPort<K> data6 = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };
  public final transient DefaultInputPort<K> data7 = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };
  public final transient DefaultInputPort<K> data8 = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };
  public final transient DefaultInputPort<K> data9 = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };
  public final transient DefaultInputPort<K> data10 = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };

  @Override
  public DefaultInputPort<K> getInputPort(int i)
  {
    DefaultInputPort<K> ret;
    switch (i) {
      case 6:
        ret = data6;
        break;
      case 7:
        ret = data7;
        break;
      case 8:
        ret = data8;
        break;
      case 9:
        ret = data9;
        break;
      case 10:
        ret = data10;
        break;
      default:
        ret = super.getInputPort(i);
        break;
    }
    return ret;
  }

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
    return 10;
  }
}
