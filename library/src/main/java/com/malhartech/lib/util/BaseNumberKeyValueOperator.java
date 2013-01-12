/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.api.StreamCodec;
import com.malhartech.engine.DefaultStreamCodec;
import com.malhartech.lib.util.KeyValPair.Codec;
import java.util.HashMap;
import javax.validation.constraints.NotNull;

/**
 *
 * Base class for operators that take in &lt;K,V extends Number&gt;. Provides basic methods for value conversion<p>
 * <br>
 * <b>Benchmarks</b>: Not done as emit is done by sub-classes<br>
 * <br>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class BaseNumberKeyValueOperator<K,V extends Number> extends BaseFilteredKeyValueOperator<K,V>

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
   * @param ctype the type to set the operator to
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
   * @param num to clone from
   * @return value as a correct sub-class (V) object
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

  public Class<? extends StreamCodec<KeyValPair<K, V>>> getKeyValPairStreamCodec()
  {
    Class c = Codec.class;
    return (Class<? extends StreamCodec<KeyValPair<K, V>>>)c;
  }
}
