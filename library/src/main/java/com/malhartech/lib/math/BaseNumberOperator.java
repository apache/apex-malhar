/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import javax.validation.constraints.NotNull;

import com.malhartech.api.BaseOperator;

/**
 *
 * Base class for operators that take in V extends Number. Provides basic methods for value conversion<p>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Over >500 million tuples/sec as all tuples are absorbed, and only one goes out at end of window<br>
 * <br>
 * @author amol
 */
public class BaseNumberOperator<V extends Number> extends BaseOperator
{
  @NotNull
  private Class <? extends Number> type = Double.class;

  public void setType(Class <V> type)
  {
    this.type = type;
  }

  public V getValue(Number num)
  {
    Number val;
    Double d = new Double(num.doubleValue());
    if (type == Double.class) {
      val = d;
    }
    else if (type == Integer.class) {
      val = d.intValue();
    }
    else if (type == Float.class) {
      val = d.floatValue();
    }
    else if (type == Long.class) {
      val = d.longValue();
    }
    else if (type == Short.class) {
      val = d.shortValue();
    }
    else {
      val = d;
    }
    return (V)val;
  }
}
