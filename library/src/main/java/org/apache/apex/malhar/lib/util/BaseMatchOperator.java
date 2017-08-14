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

  public enum ComparatorFunction
  {
    LTE, LT, EQ, NEQ, GT, GTE
  }

  ComparatorFunction comparator = ComparatorFunction.EQ;

  /**
   * The key from the input tuple whose value will be matched.
   *
   * @param key
   */
  public void setKey(K key)
  {
    this.key = key;
  }

  /**
   * Gets the key from the input tuple whose value will be matched.
   *
   * @return key
   */
  @NotNull()
  public K getKey()
  {
    return key;
  }

  /**
   * The threshold against which a comparison is done.
   *
   * @param value
   */
  public void setValue(double value)
  {
    this.value = value;
  }

  /**
   * Gets the threshold against which a comparison is done.
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
    switch (comparator) {
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

  /**
   * The comparator function to use.
   * @param type The comparator function to user.
   */
  public void setType(ComparatorFunction type)
  {
    this.comparator = type;
  }

  /**
   * The comparator function.
   * @return The comparator function.
   */
  public ComparatorFunction getType()
  {
    return comparator;
  }

  /**
   * Setter function for compare comparator. Allowed values are lte, lt, eq, ne, gt,
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

  /**
   * Gets the comparator function used.
   * @return The comparator function used.
   */
  public ComparatorFunction getComparator()
  {
    return comparator;
  }

  /**
   * The comparator function used.
   * @param comparator The comparator used.
   */
  public void setComparator(ComparatorFunction comparator)
  {
    this.comparator = comparator;
  }

  public void setTypeLT()
  {
    comparator = ComparatorFunction.LT;
  }

  public void setTypeLTE()
  {
    comparator = ComparatorFunction.LTE;
  }

  public void setTypeEQ()
  {
    comparator = ComparatorFunction.EQ;
  }

  public void setTypeNEQ()
  {
    comparator = ComparatorFunction.NEQ;
  }

  public void setTypeGT()
  {
    comparator = ComparatorFunction.GT;
  }

  public void setTypeGTE()
  {
    comparator = ComparatorFunction.GTE;
  }
}
