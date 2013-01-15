/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.storm.exclamation;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class SquareNumberSpout implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(SquareNumberSpout.class);
  public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>(this);
  public transient OperatorContext context;
  final Random rand = new Random(System.nanoTime());

  @Override
  public void emitTuples()
  {
    Integer num = rand.nextInt(30000 + 1);
    output.emit(num * num);
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.context = context;
  }

  @Override
  public void teardown()
  {
  }
}
