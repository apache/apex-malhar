/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.api.BaseOperator;

/**
 * Abstract class for basic operators that allows cloneKey for enabling users to use mutable objects
 *
 * @author amol<br>
 *
 */
abstract public class BaseKeyOperator<K> extends BaseOperator
{
  /**
   * Override this call in case you have mutable objects. By default the objects are assumed to be immutable
   * @param k to be cloned
   * @return k as is
   */
  public K cloneKey(K k)
  {
    return k;
  }
}
