/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

/**
 * Counter counts the number of tuples delivered to it in each window and emits the count.<p>
 * This is a pass through operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Object<br>
 * <b>output</b>: emits Integer<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Counter operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 80 Million tuples/s</td><td>emits one tuple per window</td><td>In-bound rate is the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Counter operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>input</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>input</i></th><th><i>output</i></th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>a</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>b</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>c</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>d</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>e</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>f</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>g</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>h</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>i</td><td>N/A</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>9</td></tr>
 * </table>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Counter implements Operator, Unifier<Integer>
{
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>(this)
  {
    @Override
    public void process(Object tuple)
    {
      count++;
    }

  };
  public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>(this)
  {
    @Override
    public Unifier<Integer> getUnifier()
    {
      return Counter.this;
    }

  };

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void process(Integer tuple)
  {
    count += tuple;
  }

  @Override
  public void endWindow()
  {
    output.emit(count);
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  private transient int count;
}
