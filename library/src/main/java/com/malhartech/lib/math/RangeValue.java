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
import java.util.ArrayList;

/**
 *
 * Takes in one stream via input port "data". At end of window sends range of all values on port "range"<p>
 * <br>
 * <b>Ports</b>
 * <b>data</b> expects V extends Number<br>
 * <b>range</b> emits ArrayList<V>(2), first Max and next Min<br>
 * <b>Compile time check</b>
 * None<br>
 * <br>
 * <b>Run time checks</b><br>
 * None<br>
 * <br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Integer: >500 million tuples/s<br>
 * Double: >500 million tuples/s<br>
 * Long: >500 million tuples/s<br>
 * Short: >500 million tuples/s<br>
 * Float: >5010 million tuples/s<br>
 * High benchmark numbers are due to the fact that the operators takes in all the tuples and only sends out one per window, i.e.
 * not bound by outbound throughput<br>
 *
 * @author amol
 */
public class RangeValue<V extends Number> extends BaseNumberOperator<V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    @Override
    public void process(V tuple)
    {
      if (low == null) {
        low = new MutableDouble(tuple.doubleValue());
      }
      else if (low.value > tuple.doubleValue()) {
        low.value = tuple.doubleValue();
      }

      if (high == null) {
        high = new MutableDouble(tuple.doubleValue());
      }
      else if (high.value < tuple.doubleValue()) {
        high.value = tuple.doubleValue();
      }
    }
  };

  @OutputPortFieldAnnotation(name = "range")
  public final transient DefaultOutputPort<ArrayList<V>> range = new DefaultOutputPort<ArrayList<V>>(this);
  MutableDouble high = null;
  MutableDouble low = null;

  @Override
  public void beginWindow()
  {
    high = null;
    low = null;
  }

  /**
   * Node only works in windowed mode. Emits all data upon end of window tuple
   */
  @Override
  public void endWindow()
  {
    if ((low != null) && (high != null)) {
      ArrayList<V> tuple = new ArrayList<V>(2);
      tuple.add(getValue(high.value));
      tuple.add(getValue(low.value));
      range.emit(tuple);
    }
  }
}
