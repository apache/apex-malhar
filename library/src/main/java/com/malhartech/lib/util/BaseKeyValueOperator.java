/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.lib.util.BaseKeyOperator;

/**
 * Abstract class for basic operators that allows cloneValue for enabling users to use mutable objects
 *
 * @author amol<br>
 *
 */
abstract public class BaseKeyValueOperator<K,V> extends BaseKeyOperator<K>
{
  /**
   * By default an immutable object is assumed. Override if V is mutable
   * @param v
   * @return v as is
   */
  public V cloneValue(V v)
  {
    return v;
  }
}
