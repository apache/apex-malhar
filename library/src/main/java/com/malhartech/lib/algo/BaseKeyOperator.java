/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.BaseOperator;

/**
 * Abstract class for basic operators that allows cloneKey for enabling users to use mutable objects
 *
 * @author amol<br>
 *
 */
abstract public class BaseKeyOperator<K> extends BaseOperator
{
  public K cloneKey(K k)
  {
    return k;
  }
}
