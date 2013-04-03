/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 * Multiplies input tuple (Number) by the value of property \"multiplier\".
 * Emits the result as Long on port \"longProduct\", as Integer on port \"integerProduct\",
 * as Double on port \"doubleProduct\", and as Float on port \"floatProduct\". This is a pass through operator<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Number<br>
 * <b>longProduct</b>: emits Long<br>
 * <b>integerProduct</b>: emits Integer<br>
 * <b>doubleProduct</b>: emits Double<br>
 * <b>floatProduct</b>: emits Float<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>multiplier</b>: Number to multiply input tuple with<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for MultiplyByConstant">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>33 million tuples/s</b></td><td>four tuples per one incoming tuple</td><td>Performance is i/o bound and directly
 * dependant on incoming tuple rate</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (multiplier = 2.0)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for MultiplyByConstant">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>input</i>::process)</th><th colspan=4>Out-bound (emit)</th></tr>
 * <tr><th><i>input</i></th><th><i>longProduct</i></th><th><i>integerProduct</i></th><th><i>doubleProduct</i></th><th><i>floatProduct</i></th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td>4</td><td>4</td><td>4.0</td><td>4.0</td></tr>
 * <tr><td>Data (process())</td><td>-12</td><td>-24</td><td>-24</td><td>-24.0</td><td>-24.0</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */

public class MultiplyByConstant extends BaseOperator
{
  public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>(this)
  {
    @Override
    public void process(Number tuple)
    {
      Long lProduct = null;
      if (longProduct.isConnected()) {
        longProduct.emit(lProduct = multiplier.longValue() * tuple.longValue());
      }

      if (integerProduct.isConnected()) {
        integerProduct.emit(lProduct == null ? (int)(multiplier.longValue() * tuple.longValue()) : lProduct.intValue());
      }

      Double dProduct = null;
      if (doubleProduct.isConnected()) {
        doubleProduct.emit(dProduct = multiplier.doubleValue() * tuple.doubleValue());
      }

      if (floatProduct.isConnected()) {
        floatProduct.emit(dProduct == null ? (float)(multiplier.doubleValue() * tuple.doubleValue()) : dProduct.floatValue());
      }
    }

  };
  public final transient DefaultOutputPort<Long> longProduct = new DefaultOutputPort<Long>(this);
  public final transient DefaultOutputPort<Integer> integerProduct = new DefaultOutputPort<Integer>(this);
  public final transient DefaultOutputPort<Double> doubleProduct = new DefaultOutputPort<Double>(this);
  public final transient DefaultOutputPort<Float> floatProduct = new DefaultOutputPort<Float>(this);

  /**
   * @param multiplier the multiplier to set
   */
  public void setMultiplier(Number multiplier)
  {
    this.multiplier = multiplier;
  }

  /**
   *
   */
  public Number getMultiplier()
  {
    return multiplier;
  }

  private Number multiplier;
}
