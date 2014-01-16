/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.db;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.util.KeyValPair;
import java.io.IOException;
import java.util.Map;

/**
 *
 * @since 0.9.3
 */
public abstract class AbstractKeyValueStoreOutputOperator<T, S extends KeyValueStore> extends BaseOperator
{
  protected S store;
  @InputPortFieldAnnotation(name = "in", optional = true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }

  };

  public S getStore()
  {
    return store;
  }

  public void setStore(S store)
  {
    this.store = store;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      store.connect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void teardown()
  {
    try {
      store.disconnect();
    }
    catch (IOException ex) {
    }
  }

  /**
   * Processes the incoming tuple.
   *
   * @param tuple
   */
  public void processTuple(T tuple)
  {
    Map<Object, Object> m = convertToMap(tuple);
    store.putAll(m);
  }

  public abstract Map<Object, Object> convertToMap(T tuple);

}
