/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

/**
 * Abstract class for basic operators. Allows cloneKey for allowing users to use mutable objects
 *
 * @author amol<br>
 *
 */
abstract public class BaseKeyValueOperator<K,V> extends BaseKeyOperator<K>
{
  public V cloneValue(V v)
  {
    return v;
  }
}
