/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 * Calculate the running average of the input numbers and emit it at the end of the window.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class RunningAverage extends BaseOperator
{
  public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>(this)
  {
    @Override
    public void process(Number tuple)
    {
      //logger.debug("before average = {}, count = {}, tuple = {}", new Object[] {average, count, tuple});
      average = ((double)(count++) / count) * average + tuple.doubleValue() / count;
      //logger.debug("after average = {}, count = {}, tuple = {}", new Object[] {average, count, tuple});
    }

  };
  public final transient DefaultOutputPort<Double> doubleAverage = new DefaultOutputPort<Double>(this);
  public final transient DefaultOutputPort<Float> floatAverage = new DefaultOutputPort<Float>(this);
  public final transient DefaultOutputPort<Long> longAverage = new DefaultOutputPort<Long>(this);
  public final transient DefaultOutputPort<Integer> integerAverage = new DefaultOutputPort<Integer>(this);

  @Override
  public void endWindow()
  {
    if (doubleAverage.isConnected()) {
      doubleAverage.emit(average);
    }

    if (floatAverage.isConnected()) {
      floatAverage.emit((float)average);
    }

    if (longAverage.isConnected()) {
      longAverage.emit((long)average);
    }

    if (integerAverage.isConnected()) {
      integerAverage.emit((int)average);
    }
  }

  double average;
  long count;
  // private static final Logger logger = LoggerFactory.getLogger(RunningAverage.class);
}
