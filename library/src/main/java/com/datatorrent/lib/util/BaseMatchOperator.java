/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

/**
 * This is an abstract operator, which provides a base implementation for operators doing comparisons.
 * <p>
 * <br>
 * Ports:<br>
 * none <br>
 * Properties:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>comp<b>: The compare function. Supported values are "lte", "lt", "eq",
 * "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * Compile time checks<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt",
 * "gte"<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * </p>
 * @displayName Base Match
 * @category Algorithmic
 * @tags compare, filter, key value, abstract
 * @since 0.3.2
 */
public class BaseMatchOperator<K, V> extends BaseKeyValueOperator<K, V>
{
  @NotNull
  private K key;

  @Pattern(regexp = "lte|lt|eq|ne|gt|gte", message = "Value has to be one of lte, lt, eq, ne, gt, gte")
  private double value = 0.0;

  public enum supported_type {
    LTE, LT, EQ, NEQ, GT, GTE
  };

  supported_type type = supported_type.EQ;

  /**
   * setter function for key
   *
   * @param key
   */
  public void setKey(K key)
  {
    this.key = key;
  }

  /**
   * getter function for key
   *
   * @return key
   */
  @NotNull()
  public K getKey()
  {
    return key;
  }

  /**
   * setter function for value
   *
   * @param value
   */
  public void setValue(double value)
  {
    this.value = value;
  }

  /**
   * getter function for value
   *
   * @return value
   */
  public double getValue()
  {
    return value;
  }

  public boolean compareValue(double value)
  {
    boolean ret;
    switch (type) {
      case LT:
        ret = value < this.value;
        break;
      case LTE:
        ret = value <= this.value;
        break;
      case EQ:
        ret = value == this.value;
        break;
      case NEQ:
        ret = value != this.value;
        break;
      case GT:
        ret = value > this.value;
        break;
      case GTE:
        ret = value >= this.value;
        break;
      default: // is EQ
        ret = value == this.value;
        break;
    }
    return ret;
  }

  public supported_type getType()
  {
    return type;
  }

  /**
   * Setter function for compare type. Allowed values are lte, lt, eq, ne, gt,
   * gte
   * <p>
   * *
   */
  @Pattern(regexp = "lte|lt|eq|ne|gt|gte", message = "Value has to be one of lte, lt, eq, ne, gt, gte")
  public void setCmp(String cmp)
  {
    if (cmp.equals("lt")) {
      setTypeLT();
    } else if (cmp.equals("lte")) {
      setTypeLTE();
    } else if (cmp.equals("eq")) {
      setTypeEQ();
    } else if (cmp.equals("ne")) {
      setTypeEQ();
    } else if (cmp.equals("gt")) {
      setTypeGT();
    } else if (cmp.equals("gte")) {
      setTypeGTE();
    } else {
      setTypeEQ();
    }
  }

  public void setTypeLT()
  {
    type = supported_type.LT;
  }

  public void setTypeLTE()
  {
    type = supported_type.LTE;
  }

  public void setTypeEQ()
  {
    type = supported_type.EQ;
  }

  public void setTypeNEQ()
  {
    type = supported_type.NEQ;
  }

  public void setTypeGT()
  {
    type = supported_type.GT;
  }

  public void setTypeGTE()
  {
    type = supported_type.GTE;
  }
}
