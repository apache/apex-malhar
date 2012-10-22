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
  supported_type default_type = supported_type.EQ;
  supported_type type = default_type;

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

  public void setCmp(String cstr) {
    if (cstr.equals("lt")) {
      type = supported_type.LT;
    }
    else if (cstr.equals("lte")) {
      type = supported_type.LTE;
    }
    else if (cstr.equals("eq")) {
      type = supported_type.EQ;
    }
    else if (cstr.equals("neq")) {
      type = supported_type.NEQ;
    }
    else if (cstr.equals("gt")) {
      type = supported_type.GT;
    }
    else if (cstr.equals("gte")) {
      type = supported_type.GTE;
    }
    else {
      type = supported_type.EQ;
    }
  }
}
