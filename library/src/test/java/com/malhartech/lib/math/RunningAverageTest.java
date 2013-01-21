/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import org.junit.Test;
import static org.junit.Assert.*;

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
  public void testEndWindow()
  {
    RunningAverage<Double> instance = new RunningAverage<Double>();
    instance.input.process(1.0);

    assertEquals("first average", 1.0, instance.average, 0.00001);
    assertEquals("first count", 1, instance.count);

    instance.input.process(2.0);

    assertEquals("second average", 1.5, instance.average, 0.00001);
    assertEquals("second count", 2, instance.count);
  }

}
