/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.datatorrent.api.BaseOperator;

/**
 * Base class for operators that allows cloneKey for enabling users to use mutable objects<p>
 *
 * @author amol<br>
 *
 */
public class BaseKeyOperator<K> extends BaseOperator
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
