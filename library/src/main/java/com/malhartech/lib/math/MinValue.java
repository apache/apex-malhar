/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableDouble;

/**
 *
 * Emits at end of window minimum of all values sub-classed from Number in the incoming stream<p>

 * <br>
 * <b>Ports</b>:
 * <b>data</b> expects V extends Number<br>
 * <b>min</b> emits V<br>
 * <br>
 * <b>Compile time checks</b>:
 * None<br>
 * <b>Run time checks</b>:
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * The operator does >500 million tuples/sec as it only emits one per end of window, and is not bounded by outbound I/O<br>
 * <br>
 *
 * @author amol
 */
public class MinValue<V extends Number> extends BaseNumberOperator<V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    /**
     * Each tuple is compared to the min and a new min (if so) is stored
     */
    @Override
    public void process(V tuple)
    {
      if (!flag) {
        low = tuple.doubleValue();
        flag = true;
      }
      else if (low > tuple.doubleValue()) {
        low = tuple.doubleValue();
      }
    }
  };

  @OutputPortFieldAnnotation(name = "min")
  public final transient DefaultOutputPort<V> min = new DefaultOutputPort<V>(this);
  double low;
  boolean flag = false;

  /**
   * Old max is reset
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    flag = false;
  }

  /**
   * Node only works in windowed mode. Emits the max. Override getValue if tuple type is mutable
   */
  @Override
  public void endWindow()
  {
    if (flag) {
      min.emit(getValue(low));
    }
  }
}
