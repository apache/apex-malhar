/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.db;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 *
 * @param <T>
 * @param <S>
 * @since 0.9.3
 */
public abstract class AbstractKeyValueStoreInputOperator<T, S extends KeyValueStore> implements InputOperator
{
  @OutputPortFieldAnnotation(name = "out")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();
  protected S store;

  protected List<Object> keys = new ArrayList<Object>();

  public S getStore()
  {
    return store;
  }

  public void setStore(S store)
  {
    this.store = store;
  }

  @Override
  public void emitTuples()
  {
    List<Object> allValues = store.getAll(keys);
    Map<Object, Object> m = new HashMap<Object, Object>();
    Iterator<Object> keyIterator = keys.iterator();
    Iterator<Object> valueIterator = allValues.iterator();
    while (keyIterator.hasNext() && valueIterator.hasNext()) {
      m.put(keyIterator.next(), valueIterator.next());
    }
    outputPort.emit(convertToTuple(m));
  }

  public abstract T convertToTuple(Map<Object, Object> o);

  @Override
  public void beginWindow(long l)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext t1)
  {
    try {
      store.connect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      store.disconnect();
    }
    catch (IOException ex) {
      // ignore
    }
  }

}
