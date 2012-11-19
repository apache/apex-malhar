/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 *
 * Adds all the values on "numerator" and "denominator" and emits quotient at end of window<p>
 * <br>
 * <b>Ports</b>:
 * <b>numerator</b> expects V extends Number<br>
 * <b>denominator</b> expects V extends Number<br>
 * <b>quotient</b> emits Double<br>
 * <br>
 * <br>
 * <b>Compile time checks</b>
 * None<br>
 * <br>
 * <b>Runtime checks</b>
 * None<br>
 * <br>
 * <b>Benchmarks</b><br>
 * <br>
 * Benchmarks:<br>
 * With Number schema the node does about >500 Million/tuples per second<br>
 * The high throughput is due to the fact that only one tuple per window is emitted<br>
 * <br>
 *
 * @author amol<br>
 *
 */

public class QuotientValue<V extends Number> extends BaseNumberValueOperator<V>
{
  @InputPortFieldAnnotation(name = "numerator")
  public final transient DefaultInputPort<V> numerator = new DefaultInputPort<V>(this)
  {
    /**
     * Adds to the numerator value
     */
    @Override
    public void process(V tuple)
    {
      nval += tuple.doubleValue();
    }
  };

  @InputPortFieldAnnotation(name = "denominator")
  public final transient DefaultInputPort<V> denominator = new DefaultInputPort<V>(this)
  {
    /**
     * Adds to the denominator value
     */
    @Override
    public void process(V tuple)
    {
      dval += tuple.doubleValue();
    }
  };

  @InputPortFieldAnnotation(name = "quotient")
  public final transient DefaultOutputPort<V> quotient = new DefaultOutputPort<V>(this);
  double nval = 0.0;
  double dval = 0.0;

  int mult_by = 1;

  public void setMult_by(int i)
  {
    mult_by = i;
  }

  /**
   * Clears denominator and numerator values
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    nval = 0.0;
    dval = 0.0;
  }

 /**
   * Generates tuple emits it as long as denomitor is not 0
   */
  @Override
  public void endWindow()
  {
    if (dval == 0) {
      return;
    }
    double val = (nval/dval)*mult_by;
    quotient.emit(getValue(val));
  }
}
