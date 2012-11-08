/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;

/**
 * Merges u to five streams with identical schema and emits tuples on to the output port in order<p>
 * <br>
 * <br>
 * <b>Tuple Schema</b>: All tuples were treated as Object
 * <b>Port Interface</b><br>
 * <b>out</b>: Output port for emitting tuples<br>
 * <b>data1</b>: Input port for receiving the 1st stream of incoming tuple<br>
 * <b>data2</b>: Input port for receiving the 2nd stream of incoming tuple<br>
 * <b>data3</b>: Input port for receiving the 3rd stream of incoming tuple<br>
 * <b>data4</b>: Input port for receiving the 4th stream of incoming tuple<br>
 * <b>data5</b>: Input port for receiving the 5th stream of incoming tuple<br>
 * <br>
 * <b>Properties</b>:
 * None
 * <br>
 * Compile time checks are:<br>
 * no checks are done. Schema check is compile/instantiation time. Not runtime
 * <br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator does >400 million tuples/sec as all tuples simply forwarded as is<br>
 *<br>
 * @author amol
 */
public class StreamMerger5<K> extends StreamMerger<K>
{
  @InputPortFieldAnnotation(name = "data3", optional=true)
  public final transient DefaultInputPort<K> data3 = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };

  @InputPortFieldAnnotation(name = "data4", optional=true)
  public final transient DefaultInputPort<K> data4 = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };

  @InputPortFieldAnnotation(name = "data5", optional=true)
  public final transient DefaultInputPort<K> data5 = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };

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
    return 5;
  }

  @Override
    public DefaultInputPort<K> getInputPort(int i)
  {
    DefaultInputPort<K> ret;
    switch(i) {
      case 3:
        ret = data3;
        break;
      case 4:
        ret = data4;
        break;
      case 5:
        ret = data5;
        break;
      default:
        ret = super.getInputPort(i);
        break;
    }
    return ret;
  }
}
