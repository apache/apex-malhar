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

/**
 *
 * Takes in one stream via input port "data". At end of window sums all values
 * and emits them on port <b>sum</b>; emits number of tuples on port <b>count</b>; and average on port <b>average</b><p>
 * <br> Values are stored in a
 * hash<br> This node only functions in a windowed stram application<br> Compile
 * time error processing is done on configuration parameters<br> input port
 * "data" must be connected<br> output port "sum" must be connected<br>
 * "windowed" has to be true<br> Run time error processing are emitted on _error
 * port. The errors are:<br> Value is not a Number<br>
 *
 * @author amol
 */
public class SumValue<V extends Number> extends BaseOperator
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

  @OutputPortFieldAnnotation(name = "sum")
  public final transient DefaultOutputPort<V> sum = new DefaultOutputPort<V>(this);
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<Integer> count = new DefaultOutputPort<Integer>(this);

  double sums = 0;
  int counts = 0;

  @Override
  public void beginWindow()
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
    if (sum.isConnected()) {
      V val = null;
      Double d = new Double(sums);
      if (val instanceof Double) {
        val = (V) d;
      }
      else if (val instanceof Integer) {
        Integer ival = d.intValue();
        val = (V) ival;
      }
      else if (val instanceof Float) {
        Float fval = d.floatValue();
        val = (V) fval;
      }
      else if (val instanceof Long) {
        Long lval = d.longValue();
        val = (V) lval;
      }
      else if (val instanceof Short) {
        Short sval = d.shortValue();
        val = (V) sval;
      }
      if (val != null) {
        sum.emit(val);
      }
    }
    if (count.isConnected()) {
      count.emit(new Integer(counts));
    }
  }
}
