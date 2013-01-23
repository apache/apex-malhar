/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class RunningAverageTest
{
  public RunningAverageTest()
  {
  }

  @Test
  public void testLogicForSmallValues()
  {
    logger.debug("small values");
    RunningAverage<Double> instance = new RunningAverage<Double>();
    instance.input.process(1.0);

    assertEquals("first average", 1.0, instance.average, 0.00001);
    assertEquals("first count", 1, instance.count);

    instance.input.process(2.0);

    assertEquals("second average", 1.5, instance.average, 0.00001);
    assertEquals("second count", 2, instance.count);
  }

  @Test
  public void testLogicForLargeValues()
  {
    logger.debug("large values");
    RunningAverage<Long> instance = new RunningAverage<Long>();
    instance.input.process(Long.MAX_VALUE);

    assertEquals("first average", Long.MAX_VALUE, (long)instance.average);

    instance.input.process(Long.MAX_VALUE);
    assertEquals("second average", Long.MAX_VALUE, (long)instance.average);
  }

  private static final Logger logger = LoggerFactory.getLogger(RunningAverageTest.class);
}
