/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Division extends BaseOperator
{
  public final transient DefaultInputPort<Number> numerator = new DefaultInputPort<Number>(this)
  {
    @Override
    public void process(Number tuple)
    {
      if (denom == null) {
        numer = tuple;
      }
      else {
        emit(tuple, denom);
        denom = null;
      }
    }

  };
  public final transient DefaultInputPort<Number> denominator = new DefaultInputPort<Number>(this)
  {
    @Override
    public void process(Number tuple)
    {
      if (numer == null) {
        denom = tuple;
      }
      else {
        emit(numer, tuple);
        numer = null;
      }
    }

  };
  public final transient DefaultOutputPort<Long> longQuotient = new DefaultOutputPort<Long>(this);
  public final transient DefaultOutputPort<Integer> integerQuotient = new DefaultOutputPort<Integer>(this);
  public final transient DefaultOutputPort<Double> doubleQuotient = new DefaultOutputPort<Double>(this);
  public final transient DefaultOutputPort<Float> floatQuotient = new DefaultOutputPort<Float>(this);
  public final transient DefaultOutputPort<Long> longRemainder = new DefaultOutputPort<Long>(this);
  public final transient DefaultOutputPort<Integer> integerRemainder = new DefaultOutputPort<Integer>(this);
  public final transient DefaultOutputPort<Double> doubleRemainder = new DefaultOutputPort<Double>(this);
  public final transient DefaultOutputPort<Float> floatRemainder = new DefaultOutputPort<Float>(this);

  public void emit(Number numer, Number denom)
  {
    Long lQuotient = null;
    Double dQuotient = null;
    Long lRemainder = null;
    Double dRemainder = null;

    if (longQuotient.isConnected()) {
      longQuotient.emit(lQuotient = numer.longValue() / denom.longValue());
    }

    if (longRemainder.isConnected()) {
      longRemainder.emit(lRemainder = numer.longValue() % denom.longValue());
    }

    if (integerQuotient.isConnected()) {
      integerQuotient.emit(lQuotient == null ? (int)(numer.longValue() % denom.longValue()) : lQuotient.intValue());
    }

    if (integerRemainder.isConnected()) {
      integerRemainder.emit(lRemainder == null ? (int)(numer.longValue() % denom.longValue()) : lRemainder.intValue());
    }

    if (doubleQuotient.isConnected()) {
      doubleQuotient.emit(dQuotient = numer.doubleValue() / denom.doubleValue());
    }

    if (doubleRemainder.isConnected()) {
      doubleRemainder.emit(dRemainder = numer.doubleValue() % denom.doubleValue());
    }

    if (floatQuotient.isConnected()) {
      floatQuotient.emit(dQuotient == null ? (float)(numer.doubleValue() / denom.doubleValue()) : dQuotient.floatValue());
    }

    if (floatRemainder.isConnected()) {
      floatRemainder.emit(dRemainder == null ? (float)(numer.doubleValue() % denom.doubleValue()) : dRemainder.floatValue());
    }
  }

  private Number numer;
  private Number denom;
}
