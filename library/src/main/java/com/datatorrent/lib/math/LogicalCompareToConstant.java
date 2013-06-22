/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Compare the constant to the incoming tuple emit it on one or more of the output ports accordingly.<p>
 * @see LogicalCompare
 * If the constant is equal to tuple, then the pair is emitted on equalTo, greaterThanEqualTo, and lessThanEqualTo ports.
 * If the constant is less than tuple, then the pair is emitted on notEqualTo, lessThan and lessThanEqualTo ports.
 * If the constant is greater than tuple, then the pair is emitted on notEqualTo, greaterThan and greaterThanEqualTo ports.
 * This is a pass through operator<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects T<br>
 * <b>equalTo</b>: emits T<br>
 * <b>notEqualTo</b>: emits T<br>
 * <b>greaterThanEqualTo</b>: emits T<br>
 * <b>greaterThan</b>: emits T<br>
 * <b>lessThanEqualTo</b>: emits T<br>
 * <b>lessThan</b>: emits T<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * @param <T> Type of each of the value in the pair<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for LogicalCompare">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>40 million tuples/s</b></td><td>six tuples emitted per one incoming tuple</td><td>Performance is i/o bound and directly
 * dependant on incoming tuple rate</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (constant = 2, T is Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for LogicalCompare">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>input</i>::process)</th><th colspan=6>Out-bound (emit)</th></tr>
 * <tr><th><i>input</i></th><th><i>equalTo</i></th><th><i>notEqualTo</i></th><th><i>greaterThan</i></th>
 * <th><i>greaterThanOrEqualTo</i></th><th><i>lessThan</i></th><th><i>lessThanOrEqualTo</i></th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>1</td><td></td><td>1</td><td>1</td><td>1</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td>2</td><td></td><td></td><td>2</td><td></td><td>2</td></tr>
 * <tr><td>Data (process())</td><td>3</td><td></td><td>3</td><td></td><td></td><td>3</td><td>3</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class LogicalCompareToConstant<T extends Comparable<? super T>> extends BaseOperator
{
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      int i = constant.compareTo(tuple);
      if (i > 0) {
          greaterThan.emit(tuple);
          greaterThanOrEqualTo.emit(tuple);
          notEqualTo.emit(tuple);
      }
      else if (i < 0) {
          lessThan.emit(tuple);
          lessThanOrEqualTo.emit(tuple);
          notEqualTo.emit(tuple);
      }
      else {
          equalTo.emit(tuple);
          lessThanOrEqualTo.emit(tuple);
          greaterThanOrEqualTo.emit(tuple);
      }
    }

  };
  public final transient DefaultOutputPort<T> equalTo = new DefaultOutputPort<T>();
  public final transient DefaultOutputPort<T> notEqualTo = new DefaultOutputPort<T>();
  public final transient DefaultOutputPort<T> lessThan = new DefaultOutputPort<T>();
  public final transient DefaultOutputPort<T> greaterThan = new DefaultOutputPort<T>();
  public final transient DefaultOutputPort<T> lessThanOrEqualTo = new DefaultOutputPort<T>();
  public final transient DefaultOutputPort<T> greaterThanOrEqualTo = new DefaultOutputPort<T>();

  /**
   * @param constant the constant to set
   */
  public void setConstant(T constant)
  {
    this.constant = constant;
  }

  /**
   * returns the value of constant
   */
  public T getConstant()
  {
    return constant;
  }


  private T constant;
}
