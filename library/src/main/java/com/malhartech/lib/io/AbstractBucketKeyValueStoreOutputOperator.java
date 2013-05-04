/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import java.util.Map;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class AbstractBucketKeyValueStoreOutputOperator<B, K, V> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "in")
  public final transient DefaultInputPort<Map<B, Map<K, V>>> input = new DefaultInputPort<Map<B, Map<K, V>>>(this)
  {
    @Override
    public void process(Map<B, Map<K, V>> t)
    {
      store(t);
    }

  };

  public abstract void store(Map<B, Map<K, V>> t);
}
