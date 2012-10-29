/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InjectConfig;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.Min;

/**
 * Abstract class for basic operators. Allows cloneKey for allowing users to use mutable objects
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
