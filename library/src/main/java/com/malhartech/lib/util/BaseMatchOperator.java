/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

/**
 *
 * Base class that sets up the operator functionality needed for match based operators<p>
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
public class BaseMatchOperator<K,V> extends BaseKeyValueOperator<K,V>
{
  @NotNull
  private K key;
  private String cmp;
  private double value = 0.0;

  public enum supported_type
  {
    LTE, LT, EQ, NEQ, GT, GTE
  };
  supported_type type = supported_type.EQ;

  @NotNull
  public void setKey(K key)
  {
    this.key = key;
  }

  @NotNull()
  public K getKey()
  {
    return key;
  }

  public void setValue(double value)
  {
    this.value = value;
  }

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
   * Setter function for compare type. Allowed values are lte, lt, eq, ne, gt, gte<p> *
   */
  @Pattern(regexp = "lte|lt|eq|ne|gt|gte", message = "Value has to be one of lte, lt, eq, ne, gt, gte")
  public void setCmp(String cmp)
  {
    if (cmp.equals("lt")) {
      setTypeLT();
    }
    else if (cmp.equals("lte")) {
      setTypeLTE();
    }
    else if (cmp.equals("eq")) {
      setTypeEQ();
    }
    else if (cmp.equals("ne")) {
      setTypeEQ();
    }
    else if (cmp.equals("gt")) {
      setTypeGT();
    }
    else if (cmp.equals("gte")) {
      setTypeGTE();
    }
    else {
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

  public HashMap<K, V> cloneTuple(HashMap<K, V> tuple)
  {
    HashMap<K, V> ret = new HashMap<K, V>(tuple.size());
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      ret.put(cloneKey(e.getKey()), cloneValue(e.getValue()));
    }
    return ret;
  }
}
