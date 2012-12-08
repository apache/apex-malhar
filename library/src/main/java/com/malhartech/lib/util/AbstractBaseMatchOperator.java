/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.util.BaseKeyOperator;
import com.malhartech.lib.util.BaseKeyValueOperator;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

/**
 *
 * An abstract class that sets up the basic operator functionality needed for match based operators<p>
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
public abstract class AbstractBaseMatchOperator<K,V extends Comparable> extends BaseKeyValueOperator<K,V>
{
  @NotNull
  private K key;
  private String cmp;
  private V value;

  public enum supported_type
  {
    LTE, LT, EQ, NEQ, GT, GTE
  };
  supported_type type = supported_type.EQ;

  /**
   * Setter function for key
   * @param key
   */
  public void setKey(K key)
  {
    this.key = key;
  }

  /**
   * getter function for key
   * @return key
   */
  @NotNull()
  public K getKey()
  {
    return key;
  }

  /**
   * setter function for value
   * @param value
   */
  public void setValue(V value)
  {
    this.value = value;
  }

  /**
   * getter function for value
   * @return
   */
  public V getValue()
  {
    return value;
  }

  @Override
  public void setup(OperatorContext context)
  {
    if (getKey() == null) {
      throw new IllegalArgumentException("Key not set");
    }
    if (getValue() == null) {
      throw new IllegalArgumentException("Value not set");
    }
  }


  /**
   * Compares with getValue() and returns the result
   * @param value
   * @return is value.compareTo(getValue())
   */
  public int compareValue(V value)
  {
    return getValue().compareTo(value);
  }

  public supported_type getType()
  {
    return type;
  }

  public boolean matchCondition(V value)
  {
    boolean ret;
    int cval = compareValue(value);
    switch (type) {
      case LT:
        ret = (cval < 0);
        break;
      case LTE:
        ret = (cval < 0) || (cval == 0);
        break;
      case EQ:
        ret = (cval == 0);
        break;
      case NEQ:
        ret = (cval != 0);
        break;
      case GT:
        ret = (cval > 0);
        break;
      case GTE:
        ret = (cval > 0) || (cval == 0);
        break;
      default: // is EQ
        ret = (cval == 0);
        break;
    }
    return ret;
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
}
