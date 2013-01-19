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
public class SquareCalculus extends SingleVariableAbstractCalculus
{
  @Override
  public double function(double dval)
  {
    return dval * dval;
  }

  @Override
  public long function(long lval)
  {
    return lval * lval;
  }

}
