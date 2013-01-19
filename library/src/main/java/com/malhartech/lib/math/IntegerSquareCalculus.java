/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

/**
 * Calculate square of the given integer.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class IntegerSquareCalculus extends AbstractUnaryCalculus<Integer, Long>
{
  @Override
  public Long function(Integer inputNumber)
  {
    return (long)inputNumber * inputNumber;
  }

}
