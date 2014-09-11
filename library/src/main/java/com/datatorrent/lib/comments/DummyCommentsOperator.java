/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.comments;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * This is a test comment on the DummyCommentsOperator class.
 *
 * detailed description of the comment
 *
 * @param <T> detail about T
 * @customTag1 custom tag1 value
 * @customTag2 custom tag2 value
 * @customTag3 1234
 * @since 1.0.5
 */
@OperatorAnnotation(partitionable = false)
public class DummyCommentsOperator<T> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "input port", optional = true)
  DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      someMethod(tuple);
    }

  };
  @OutputPortFieldAnnotation(name = "output port", optional = false)
  DefaultOutputPort<T> output = new DefaultOutputPort<T>();

  /**
   * Some method description.
   * detailed description
   *
   * @param tuple details about tuple
   */
  public void someMethod(T tuple)
  {
    output.emit(tuple);
  }

}
