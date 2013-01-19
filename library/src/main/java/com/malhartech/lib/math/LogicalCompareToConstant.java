/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.util.Pair;
import javax.validation.constraints.NotNull;

/**
 * Compare the tuple to a constant tuple and emit it on one or more of the output ports accordingly.
 *
 * @see LogicalCompare
 * @param <T>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class LogicalCompareToConstant<T extends Comparable<? super T>> extends BaseOperator
{
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      int i = constant.compareTo(tuple);
      if (i > 0) {
        if (greaterThan.isConnected()) {
          greaterThan.emit(tuple);
        }

        if (greaterThanOrEqualTo.isConnected()) {
          greaterThanOrEqualTo.emit(tuple);
        }
      }
      else if (i < 0) {
        if (lessThan.isConnected()) {
          lessThan.emit(tuple);
        }

        if (lessThanOrEqualTo.isConnected()) {
          lessThanOrEqualTo.emit(tuple);
        }
      }
      else {
        if (equalTo.isConnected()) {
          equalTo.emit(tuple);
        }

        if (lessThanOrEqualTo.isConnected()) {
          lessThanOrEqualTo.emit(tuple);
        }

        if (greaterThanOrEqualTo.isConnected()) {
          greaterThanOrEqualTo.emit(tuple);
        }
      }
    }

  };
  public final transient DefaultOutputPort<T> equalTo = new DefaultOutputPort<T>(this);
  public final transient DefaultOutputPort<T> lessThan = new DefaultOutputPort<T>(this);
  public final transient DefaultOutputPort<T> greaterThan = new DefaultOutputPort<T>(this);
  public final transient DefaultOutputPort<T> lessThanOrEqualTo = new DefaultOutputPort<T>(this);
  public final transient DefaultOutputPort<T> greaterThanOrEqualTo = new DefaultOutputPort<T>(this);

  /**
   * @param constant the constant to set
   */
  public void setConstant(T constant)
  {
    this.constant = constant;
  }

  private T constant;
}
