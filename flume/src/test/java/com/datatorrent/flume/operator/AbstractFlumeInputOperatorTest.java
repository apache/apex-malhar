/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.operator;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class AbstractFlumeInputOperatorTest
{
  public AbstractFlumeInputOperatorTest()
  {
  }

  @Test
  public void testThreadLocal()
  {
    ThreadLocal<Set<Integer>> tl = new ThreadLocal<Set<Integer>>()
    {
      @Override
      protected Set<Integer> initialValue()
      {
        return new HashSet<Integer>();
      }

    };
    Set<Integer> get1 = tl.get();
    get1.add(1);
    assertTrue("Just Added Value", get1.contains(1));

    Set<Integer> get2 = tl.get();
    assertTrue("Previously added value", get2.contains(1));
  }

}
