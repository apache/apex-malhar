/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

/**
 * Base class for operators that allows cloneValue and cloneKey for enabling users to use mutable objects<p>
 *
 * @author amol<br>
 *
 */
public class BaseKeyValueOperator<K,V> extends BaseKeyOperator<K>
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
