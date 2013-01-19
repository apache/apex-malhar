/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import javax.validation.constraints.NotNull;

/**
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

  private Number multiplier;
}
