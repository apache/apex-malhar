/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 * Merges two streams with identical schema and emits the tuples to the output port in order<br>
 * The aim is to simply getUnifier two streams of same schema type<p>
 * <br>
 * This module may not be needed once dynamic getUnifier is supported by Stram
 * <br>
 * Benchmarks: This node has been benchmarked at over 18 million tuples/second in local/inline mode<br>
 *
 * <b>Tuple Schema</b>: All tuples were treated as Object
 * <b>Port Interface</b><br>
 * <b>out</b>: Output port for emitting tuples<br>
 * <b>data1</b>: Input port for receiving the first stream of incoming tuple<br>
 * <b>data2</b>: Input port for receiving the second stream of incoming tuple<br>
 * <br>
 * <b>Properties</b>:
 * None
 * <br>
 * Compile time checks are:<br>
 * no checks are done. Schema check is compile/instantiation time. Not runtime<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator does >400 million tuples/sec as all tuples simply forwarded as is<br>
 * @author amol
 */
public class StreamMerger<K> extends BaseOperator
{
  public final transient DefaultInputPort<K> data1 = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };
  public final transient DefaultInputPort<K> data2 = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };
  public final transient DefaultOutputPort<K> out = new DefaultOutputPort<K>(this);

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
