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
 * Emits the range of values at the end of window<p>
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
 * The operator does >500 million tuples/sec as it only emits one per end of window, and is not bounded by outbound I/O. It uses a Mutable number and thus avoids memory allocation<br>
 * <br>
 * @author amol
 */
public class RangeValue<V extends Number> extends BaseNumberValueOperator<V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    /**
     * Process each tuple to compute new high and low
     */
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

  /**
   * Clears the values
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    high = null;
    low = null;
  }

  /**
   * Emits the range. If no tuple was received in the window, no emit is done
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
