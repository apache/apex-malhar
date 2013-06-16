/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 * Calculate the running average of the input numbers and emit it at the end of the window.<p>
 * This is an end of window operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Number<br>
 * <b>longAverage</b>: emits Long<br>
 * <b>integerAverage</b>: emits Integer<br>
 * <b>doubleAverage</b>: emits Double<br>
 * <b>floatAverage</b>: emits Float<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for RunningAverage">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>55 million tuples/s</b></td><td>four tuples per window</td><td>Performance is input i/o bound and directly
 * dependant on incoming tuple rate</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for RunningAverage">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=4>Out-bound (emit)</th></tr>
 * <tr><th><i>input</i></th><th><i>longAverage</i></th><th><i>integerAverage</i></th><th><i>doubleAverage</i></th><th><i>floatAverage</i></th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>3</td><td></td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>1</td><td></td><td></td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>2</td><td>2</td><td>2.0</td><td>2.0</td></tr>
 * </table>
 * <br>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class RunningAverage extends BaseOperator
{
  public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>(this)
  {
    @Override
    public void process(Number tuple)
    {
      //logger.debug("before average = {}, count = {}, tuple = {}", new Object[] {average, count, tuple});
      average = ((double)(count++) / count) * average + tuple.doubleValue() / count;
      //logger.debug("after average = {}, count = {}, tuple = {}", new Object[] {average, count, tuple});
    }

  };
  public final transient DefaultOutputPort<Double> doubleAverage = new DefaultOutputPort<Double>(this);
  public final transient DefaultOutputPort<Float> floatAverage = new DefaultOutputPort<Float>(this);
  public final transient DefaultOutputPort<Long> longAverage = new DefaultOutputPort<Long>(this);
  public final transient DefaultOutputPort<Integer> integerAverage = new DefaultOutputPort<Integer>(this);

  @Override
  public void endWindow()
  {
    if (doubleAverage.isConnected()) {
      doubleAverage.emit(average);
    }

    if (floatAverage.isConnected()) {
      floatAverage.emit((float)average);
    }

    if (longAverage.isConnected()) {
      longAverage.emit((long)average);
    }

    if (integerAverage.isConnected()) {
      integerAverage.emit((int)average);
    }
  }

  double average;
  long count;
  // private static final Logger logger = LoggerFactory.getLogger(RunningAverage.class);
}
