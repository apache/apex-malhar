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

      if (passThrough == true && (collection.size() == size)) {
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

  @Min(1)
  public int getSize()
  {
    return size;
  }

  /**
   * If set to true, the Collection is emitted at the endWindow call, thereby making this operator stateless. By default flush is false, i.e.
   * the output tuple is guaranteed to be of size set by the user. By default this is a stateful operator.
   * @param flag
   */
  public void setFlushWindow(boolean flag)
  {
    flushWindow = flag;
  }

  /**
   * Sets pass through. Default is true, i.e. Collection is emitted as soon as number of elements is equal to size. If pass through is false,
   * then size is not relevant, and the Collection is only emitted in endWindow as long as Collection has at least one element.
   * @param flag
   */
  public void setPassThrough(boolean flag)
  {
    passThrough = flag;
  }

  public boolean getPassThrough(boolean flag)
  {
    return passThrough;
  }

  public boolean getFlushWindow()
  {
    return flushWindow;
  }

  public abstract Collection<T> getNewCollection(int size);

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    if (size != 0 && collection != null) {
      if (flushWindow == true || passThrough == false) {
        output.emit(collection);
        collection = null;
      }
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
  @Min(1)
  private int size = 1;
  private boolean flushWindow = false;
  private boolean passThrough = true;
}
