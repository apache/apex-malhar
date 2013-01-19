/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.DefaultInputPort;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class SingleVariableAbstractCalculus extends AbstractFunction
{
  public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>(this)
  {
    @Override
    public void process(Number tuple)
    {
      Double dResult = null;
      if (doubleResult.isConnected()) {
        doubleResult.emit(dResult = function(tuple.doubleValue()));
      }

      if (floatResult.isConnected()) {
        floatResult.emit(dResult == null ? (float)function(tuple.doubleValue()) : dResult.floatValue());
      }

      Long lResult = null;
      if (longResult.isConnected()) {
        longResult.emit(lResult = function(tuple.longValue()));
      }

      if (integerResult.isConnected()) {
        integerResult.emit(lResult == null ? (int)function(tuple.longValue()) : lResult.intValue());
      }
    }

  };

  /**
   * Transform the input into the output after applying appropriate mathematical function to it.
   *
   * @param val
   * @return result of the function
   */
  public abstract double function(double val);

  /**
   *
   * @param val
   * @return
   */
  public abstract long function(long val);

}
