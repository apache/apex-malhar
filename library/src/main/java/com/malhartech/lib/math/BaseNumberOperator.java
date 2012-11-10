/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import javax.validation.constraints.NotNull;

/**
 *
 * Base class for operators that take in V extends Number. Provides basic methods for value conversion<p>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Over >500 million tuples/sec as all tuples are absorbed, and only one goes out at end of window<br>
 * <br>
 *
 * @author amol
 */
public class BaseNumberOperator<V extends Number> extends BaseOperator
{
  public enum V_TYPE
  {
    DOUBLE, INTEGER, FLOAT, LONG, SHORT, UNKNOWN
  };
  @NotNull
  V_TYPE type = V_TYPE.DOUBLE;

  /**
   * This call ensures that type enum is set at setup time. At run time a switch statement suffices
   * If you derive your
   * @param type
   */

  public void setType(Class<V> ctype)
  {
    if (ctype == Double.class) {
      type = V_TYPE.DOUBLE;
    }
    else if (ctype == Integer.class) {
      type = V_TYPE.INTEGER;
    }
    else if (ctype == Float.class) {
      type = V_TYPE.FLOAT;
    }
    else if (ctype == Long.class) {
      type = V_TYPE.LONG;
    }
    else if (ctype == Short.class) {
      type = V_TYPE.SHORT;
    }
    else {
      type = V_TYPE.UNKNOWN;
    }
  }

  /**
   * This is a constructor provided to clone Number. Since V is instantiation time and not known during module creation, a copy
   * is needed. Currently only the following are supported: Double, Integer, Float, Long, and Short. If you derive your own class from Number
   * then you need to override getValue to help make a correct copy.
   * @param num
   * @return
   */
  public V getValue(Number num)
  {
    Number val;
    switch (type) {
      case DOUBLE:
        val = new Double(num.doubleValue());
        break;
      case INTEGER:
        val = new Integer(num.intValue());
        break;
      case FLOAT:
        val = new Float(num.floatValue());
        break;
      case LONG:
        val = new Long(num.longValue());
        break;
      case SHORT:
        val = new Short(num.shortValue());
        break;
      default:
        val = new Double(num.doubleValue());
        break;
    }
    return (V)val;
  }
}
