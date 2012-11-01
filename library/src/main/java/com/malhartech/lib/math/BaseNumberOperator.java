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
import javax.validation.constraints.NotNull;

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
  Class <V> type;

  public void setType(Class <V> type)
  {
    this.type = type;
  }

  public V getValue(Number num)
  {
    V val;
    Double d = new Double(num.doubleValue());
    if (type == Double.class) {
      val = (V)d;
    }
    else if (type == Integer.class) {
      Integer ival = d.intValue();
      val = (V)ival;
    }
    else if (type == Float.class) {
      Float fval = d.floatValue();
      val = (V)fval;
    }
    else if (type == Long.class) {
      Long lval = d.longValue();
      val = (V)lval;
    }
    else if (type == Short.class) {
      Short sval = d.shortValue();
      val = (V)sval;
    }
    else {
      val = (V) d;
    }
    return val;
  }
}
