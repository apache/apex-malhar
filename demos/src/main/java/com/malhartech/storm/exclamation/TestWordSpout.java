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
public class TestWordSpout implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(TestWordSpout.class);
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>(this);

  @Override
  public void emitTuples()
  {
    try {
      Thread.sleep(100);
    }
    catch (InterruptedException ie) {
      logger.debug("interrupted while sleeping for 100 ms!");
    }

    final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
    final Random rand = new Random();
    final String word = words[rand.nextInt(words.length)];
    output.emit(word);
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
  }

  @Override
  public void teardown()
  {
  }

}
