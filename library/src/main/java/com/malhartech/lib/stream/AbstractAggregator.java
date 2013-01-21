/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator;
import java.util.Collection;
import javax.validation.constraints.Min;

// should this be partitionable?
/**
 *
 * @param <T>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractAggregator<T> implements Operator
{
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      if (collection == null) {
        collection = getNewCollection(size);
      }

      collection.add(tuple);

      if (collection.size() == size) {
        output.emit(collection);
        collection = null;
      }
    }

  };
  public final transient DefaultOutputPort<Collection<T>> output = new DefaultOutputPort<Collection<T>>(this);

  /**
   * Set the size of the collection.
   *
   * If set to zero, the collection collects all the tuples within a window and
   * emits the collection as 1 output tuple at the end of the window.
   * If set to positive value, it collects the collection as soon as the size
   * of the collection reaches the size.
   *
   * @param size the size to set
   */
  public void setSize(int size)
  {
    this.size = size;
  }

  public abstract Collection<T> getNewCollection(int size);

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    if (size == 0 && collection != null) {
      output.emit(collection);
      collection = null;
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  protected Collection<T> collection;
  @Min(0)
  private int size;
}
