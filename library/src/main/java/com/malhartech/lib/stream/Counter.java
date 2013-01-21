/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator;
import com.malhartech.api.Operator.Unifier;

/**
 * Counter counts the number of tuples delivered to it in each window and emits the count.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Counter implements Operator//, Unifier<Integer>
{
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>(this)
  {
    @Override
    public void process(Object tuple)
    {
      count++;
    }

  };
  public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>(this)
  {
//    @Override
//    public Unifier<Integer> getUnifier()
//    {
//      return Counter.this;
//    }

  };

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

//  @Override
  public void merge(Integer tuple)
  {
    count += tuple;
  }

  @Override
  public void endWindow()
  {
    output.emit(count);
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  private transient int count;
}
