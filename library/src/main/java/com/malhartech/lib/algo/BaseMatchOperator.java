/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.BaseOperator;

/**
 *
 * An abstract class that sets up the basic operator functionality needed for match based operators
 * <br>
 * Ports:<br>
 * none
 * <br>
 * Properties:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>comp<b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * Compile time checks<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * Run time checks<br>
 * none<br>
 *
 * @author amol
 */

public class BaseMatchOperator<K> extends BaseOperator
{
  K key;
  String cmp;
  double default_value = 0.0;
  double value = default_value;

  public enum supported_type {LTE, LT, EQ, NEQ, GT, GTE};
  supported_type type = supported_type.EQ;

  public void setKey(K str) {
    key = str;
  }

  public K getKey()
  {
    return key;
  }

  public void setValue(double v) {
    value = v;
  }

  public double getValue()
  {
    return value;
  }

  public supported_type getType()
  {
    return type;
  }

  public void steTypeLT()
  {
    type = supported_type.LT;
  }

  public void steTypeLTE()
  {
    type = supported_type.LTE;
  }

  public void steTypeEQ()
  {
    type = supported_type.EQ;
  }

  public void steTypeNEQ()
  {
    type = supported_type.NEQ;
  }

  public void steTypeGT()
  {
    type = supported_type.GT;
  }

   public void steTypeGTE()
  {
    type = supported_type.GTE;
  }
}
