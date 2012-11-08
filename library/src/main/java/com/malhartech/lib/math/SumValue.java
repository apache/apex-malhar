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
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;

/**
 *
 * Emits the sum, average, and count of values at the end of window<p>
 * This is an end of window operator<br>
 * <b>Ports</b>:
 * <b>data</b> expects V extends Number<br>
 * <b>sum</b> emits V extends Number<br>
 * <b>count</b> emits Integer</b>
 * Compile time checks<br>
 * None<br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator does >500 million tuples/sec as all tuples are processed, but only one goes out at end of window<br>
 * <br>
 * @author amol
 */
public class SumValue<V extends Number> extends BaseNumberOperator<V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    @Override
    public void process(V tuple)
    {
      sums += tuple.doubleValue();
      counts++;
    }
  };

  @OutputPortFieldAnnotation(name = "sum", optional=true)
  public final transient DefaultOutputPort<V> sum = new DefaultOutputPort<V>(this);
  @OutputPortFieldAnnotation(name = "count", optional=true)
  public final transient DefaultOutputPort<Integer> count = new DefaultOutputPort<Integer>(this);

  double sums = 0;
  int counts = 0;

  @Override
  public void beginWindow(long windowId)
  {
    sums = 0;
    counts = 0;
  }

  /**
   * Node only works in windowed mode. Emits all data upon end of window tuple
   */
  @Override
  public void endWindow()
  {
    // May want to send out only if count != 0
    if (sum.isConnected()) {
      sum.emit(getValue(sums));
    }
    if (count.isConnected()) {
      count.emit(new Integer(counts));
    }
  }
}
