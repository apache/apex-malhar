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
 * Takes in one stream via input port "data". At end of window sums all values
 * and emits them on port <b>sum</b>; emits number of tuples on port <b>count</b><p>
 * This is an end of window operator<br>
 * <b>Ports</b>:
 * <b>data</b> expects V extends Number<br>
 * <b>sum</b> emits V extends Number<br>
 * <b>count</b> emits Integer</b>
 * Compile time checks<br>
 * None<br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Over 100 million tuples/sec as all tuples are absorbed, and only one goes out at end of window<br>
 * <br>
 * @author amol
 */
public class BaseNumberOperator<V extends Number> extends BaseOperator
{
  @NotNull
  Class <V> type;
  @NotNull
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
