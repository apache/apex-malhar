/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in one stream via input port "data". At end of window sends range of all values
 * for each key and emits them on port "range"<p>
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
 * Integer: 8 million tuples/s<br>
 * Double: 8 million tuples/s<br>
 * Long: 8 million tuples/s<br>
 * Short: 8 million tuples/s<br>
 * Float: 8 million tuples/s<br>
 *
 * @author amol
 */
public class RangeValue<V extends Number> extends BaseOperator
{
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    @Override
    public void process(V tuple)
    {
      if (low == null) {
        low = tuple;
      }
      if (high == null) {
        high = tuple;
      }

      if (low.doubleValue() > tuple.doubleValue()) {
        low = tuple;
      }
      if (high.doubleValue() < tuple.doubleValue()) {
        high = tuple;
      }
    }
  };
  public final transient DefaultOutputPort<ArrayList<V>> range = new DefaultOutputPort<ArrayList<V>>(this);
  V high = null;
  V low = null;

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
      tuple.add(high);
      tuple.add(low);
      range.emit(tuple);
    }
  }
}
