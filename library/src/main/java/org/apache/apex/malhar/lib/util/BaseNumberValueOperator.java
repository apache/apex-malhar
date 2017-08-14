/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.util;

import javax.validation.constraints.NotNull;

/**
 * This is an abstract operator, which provides a base implementation for operators which work with numbers.
 * <p>
 * Provides basic methods for value conversion.
 * <br>
 * <b>Benchmarks</b>: Not done as there is emit tuple is done by sub-classes<br>
 * </p>
 * @displayName Base Number Value
 * @category Algorithmic
 * @tags numeric
 * @since 0.3.2
 */
public class BaseNumberValueOperator<V extends Number> extends BaseKeyOperator<V>

{
  public enum V_TYPE
  {
    DOUBLE, INTEGER, FLOAT, LONG, SHORT, UNKNOWN
  }

  @NotNull
  protected V_TYPE type = V_TYPE.DOUBLE;

  public V_TYPE getVType()
  {
    return type;
  }

  public void setVType(V_TYPE type)
  {
    this.type = type;
  }

  /**
   * Gets the type.
   * @omitFromUI
   * @return The type.
   */
  public V_TYPE getType()
  {
    return type;
  }

  /**
   * This call ensures that type enum is set at setup time. At run time a switch statement suffices
   * If you derive your
   * @param ctype the type to set the operator to
   */

  public void setType(Class<V> ctype)
  {
    if (ctype == Double.class) {
      type = V_TYPE.DOUBLE;
    } else if (ctype == Integer.class) {
      type = V_TYPE.INTEGER;
    } else if (ctype == Float.class) {
      type = V_TYPE.FLOAT;
    } else if (ctype == Long.class) {
      type = V_TYPE.LONG;
    } else if (ctype == Short.class) {
      type = V_TYPE.SHORT;
    } else {
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
  @SuppressWarnings("unchecked")
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
