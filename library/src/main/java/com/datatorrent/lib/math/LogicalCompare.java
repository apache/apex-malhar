/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.Pair;

/**
 * Given a pair<T,T> object which contains 2 values of the comparable property, compare the first value with the second
 * and emit the pair on appropriate port denoting the result of the comparison.
 * If the first value is equal to second value, then the pair is emitted on equalTo, greaterThanEqualTo, and lessThanEqualTo ports.
 * If the first value is less than second value, then the pair is emitted on notEqualTo, lessThan and lessThanEqualTo ports.
 * If the first value is greater than second value, then the pair is emitted on notEqualTo, greaterThan and greaterThanEqualTo ports.
 * This is a pass through operator<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Pair&lt;T,T&gt;<br>
 * <b>equalTo</b>: emits Pair&lt;T,T&gt;<br>
 * <b>notEqualTo</b>: emits Pair&lt;T,T&gt;<br>
 * <b>greaterThanEqualTo</b>: emits Pair&lt;T,T&gt;<br>
 * <b>greaterThan</b>: emits Pair&lt;T,T&gt;<br>
 * <b>lessThanEqualTo</b>: emits Pair&lt;T,T&gt;<br>
 * <b>lessThan</b>: emits Pair&lt;T,T&gt;<br>
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
 * <b>Function Table</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for LogicalCompare">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>input</i>::process)</th><th colspan=6>Out-bound (emit)</th></tr>
 * <tr><th><i>input</i></th><th><i>equalTo</i></th><th><i>notEqualTo</i></th><th><i>greaterThan</i></th>
 * <th><i>greaterThanOrEqualTo</i></th><th><i>lessThan</i></th><th><i>lessThanOrEqualTo</i></th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{2,1}</td><td></td><td>{2,1}</td><td>{2,1}</td><td>{2,1}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{2,2}</td><td>{2,2}</td><td></td><td></td><td>{2,2}</td><td></td><td>{2,2}</td></tr>
 * <tr><td>Data (process())</td><td>{2,3}</td><td></td><td>{2,3}</td><td></td><td></td><td>{2,3}</td><td>{2,3}</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 *
 */
public abstract class LogicalCompare<T extends Comparable<? super T>> extends BaseOperator
{
  public final transient DefaultInputPort<Pair<T, T>> input = new DefaultInputPort<Pair<T, T>>()
  {
    @Override
    public void process(Pair<T, T> tuple)
    {
      int i = tuple.first.compareTo(tuple.second);
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
  public final transient DefaultOutputPort<Pair<T, T>> equalTo = new DefaultOutputPort<Pair<T, T>>();
  public final transient DefaultOutputPort<Pair<T, T>> notEqualTo = new DefaultOutputPort<Pair<T, T>>();
  public final transient DefaultOutputPort<Pair<T, T>> lessThan = new DefaultOutputPort<Pair<T, T>>();
  public final transient DefaultOutputPort<Pair<T, T>> greaterThan = new DefaultOutputPort<Pair<T, T>>();
  public final transient DefaultOutputPort<Pair<T, T>> lessThanOrEqualTo = new DefaultOutputPort<Pair<T, T>>();
  public final transient DefaultOutputPort<Pair<T, T>> greaterThanOrEqualTo = new DefaultOutputPort<Pair<T, T>>();
}
