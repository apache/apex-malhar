/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import java.util.Collection;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class IntegerSumCalculus extends AbstractUnaryCalculus<Collection<Integer>, Long>
{
  @Override
  public Long function(Collection<Integer> inputNumbers)
  {
    Long l = 0L;
    for (Integer i: inputNumbers) {
      l += i;
    }
    return l;
  }

}
