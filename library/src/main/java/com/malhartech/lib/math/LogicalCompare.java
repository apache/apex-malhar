/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.util.Pair;

/**
 * Given a pair object which contains 2 values of the comparable property, compare the first value with the second
 * and emit the pair on appropriate port denoting the result of the comparison.
 * If the result of comparing two values is 0 (zero), then the pair is emitted on equalTo, greaterThanEqualTo, and lessThanEqualTo ports.
 * If the result is less than 0, then the pair is emitted on lessThan and lessThanEqualTo ports.
 * If the result is greater than 0, then the pair is emitted on greaterThan and greaterThanEqualTo ports.
 *
 * @param <T> Type of each of the value in the pair.
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class LogicalCompare<T extends Comparable<? super T>> extends BaseOperator
{
  public final transient DefaultInputPort<Pair<T, T>> input = new DefaultInputPort<Pair<T, T>>(this)
  {
    @Override
    public void process(Pair<T, T> tuple)
    {
      int i = tuple.first.compareTo(tuple.second);
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
  public final transient DefaultOutputPort<Pair<T, T>> equalTo = new DefaultOutputPort<Pair<T, T>>(this);
  public final transient DefaultOutputPort<Pair<T, T>> lessThan = new DefaultOutputPort<Pair<T, T>>(this);
  public final transient DefaultOutputPort<Pair<T, T>> greaterThan = new DefaultOutputPort<Pair<T, T>>(this);
  public final transient DefaultOutputPort<Pair<T, T>> lessThanOrEqualTo = new DefaultOutputPort<Pair<T, T>>(this);
  public final transient DefaultOutputPort<Pair<T, T>> greaterThanOrEqualTo = new DefaultOutputPort<Pair<T, T>>(this);
}
