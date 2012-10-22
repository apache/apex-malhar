/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 *
 * Takes in one stream via input port "data". At end of window sends minimum of all values
 * for each key and emits them on port "min"<p>
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
 * <br>
 *
 * @author amol
 */
public class MinValue<V extends Number> extends BaseOperator
{
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    @Override
    public void process(V tuple)
    {
      if (low == null) {
        low = tuple;
      }
      else if (low.doubleValue() > tuple.doubleValue()) {
        low = tuple;
      }
    }
  };
  public final transient DefaultOutputPort<V> min = new DefaultOutputPort<V>(this);
  V low = null;

  @Override
  public void beginWindow()
  {
    low = null;
  }

  /**
   * Node only works in windowed mode. Emits all data upon end of window tuple
   */
  @Override
  public void endWindow()
  {
    if (low != null) {
      min.emit(low);
    }
  }
}
