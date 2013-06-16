/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.multiwindow;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import javax.validation.constraints.Min;

/**
 *
 * A sliding window class that lets users access past N-1 window states<p>
 * N is a property. The default behavior is just a passthrough, i.e. the operator does not do any processing on its own.
 * Users are expected to extend this class and add their specific processing. Users have to define their own output port(s)<br>
 * This module is a pass through or end of window as per users choice<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects T (any POJO)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>N</b>: Number of windows to keep state on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Benchmark done for a String type with ArrayList of all tuples in that window as the window state. Since the benchmark is only for
 * state save and retrieval, the actual processing is not counted<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractSlidingWindow&lt;T&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>400 million windows</b></td><td>tuple is emitted as is</td>
 * <td>The tuple processing was just pass through. The benchmark only was for state save of a String. The processing time would directly
 * depend on actual tuple processing. The cost of state management is negligible</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table</b>: Not relevant as it is a AbstractSlidingWindow class<br>
 * <b>State Table</b>: For state of type String; and n = 3; Assume saveWindowState(String) called in endWindow() by user's operator extended from AbstractSlidingWindow<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="State table for AbstractSlidingWindow&lt;T&gt; operator template">
 * <tr><th>Window Id</th><th>saveWindowState(Object o)</th>
 * <th>Values returned by getWindowState(i) before saveWindowState(o) is called</th><th>Values returned by getWindowState(i) after saveWindowState(o) is called</th></tr>
 * <tr><td>0</td><td>o = "s0"</td><td>i=2 returns null<br>i=1 returns null<br>i=0 returns null</td><td>i=2 returns "s0"<br>i=1 returns null<br>i=0 returns null</td></tr>
 * <tr><td>1</td><td>o = "s1"</td><td>i=2 returns null<br>i=1 returns "s0"<br>i=0 returns null</td><td>i=2 returns "s1"<br>i=1 returns "s0"<br>i=0 returns null</td></tr>
 * <tr><td>2</td><td>o = "s2"</td><td>i=2 returns null<br>i=1 returns "s1"<br>i=0 returns "s0"</td><td>i=2 returns "s2"<br>i=1 returns "s1"<br>i=0 returns "s0"</td></tr>
 * <tr><td>3</td><td>o = "s3"</td><td>i=2 returns null<br>i=1 returns "s2"<br>i=0 returns "s1"</td><td>i=2 returns "s3"<br>i=1 returns "s2"<br>i=0 returns "s1"</td></tr>
 * <tr><td>4</td><td>o = "s4"</td><td>i=2 returns null<br>i=1 returns "s3"<br>i=0 returns "s2"</td><td>i=2 returns "s4"<br>i=1 returns "s3"<br>i=0 returns "s2"</td></tr>
 * <tr><td>5</td><td>o = "s5"</td><td>i=2 returns null<br>i=1 returns "s4"<br>i=0 returns "s3"</td><td>i=2 returns "s5"<br>i=1 returns "s4"<br>i=0 returns "s3"</td></tr>
 * <tr><td>6</td><td>o = "s6"</td><td>i=2 returns null<br>i=1 returns "s5"<br>i=0 returns "s4"</td><td>i=2 returns "s6"<br>i=1 returns "s5"<br>i=0 returns "s4"</td></tr>
 * </table>
 *
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public abstract class AbstractSlidingWindow<T> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<T> data = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      processDataTuple(tuple);
    }
  };

  protected Object[] states = null;
  protected int currentstate = -1;

  @Min(2)
  int n = 2;

  /**
   * getter function for n (number of previous window states
   *
   * @return n
   */
  @Min(2)
  public int getN()
  {
    return n;
  }

  /**
   * setter function for n
   *
   * @param i
   */
  void setN(int i)
  {
    n = i;
  }

  abstract void processDataTuple(T tuple);

  /**
   * Saves Object o as current window state. Calling this api twice in same window would simply overwrite the previous call.
   * This can be called during processDataTuple, or endWindow. Usually it is better to call it in endWindow.
   *
   * @param o
   */
  public void saveWindowState(Object o)
  {
    states[currentstate] = o;
  }

  /**
   * Can access any previous window state upto n-1 (0 is current, n-1 is the N-1th previous)
   *
   * @param i the previous window whose state is accessed
   * @return Object
   */
  public Object getWindowState(int i)
  {
    if (i >= getN()) {
      return null;
    }
    int j = currentstate;
    while (i != (getN()-1)) {
      j--;
      if (j < 0) {
        j = getN()-1;
      }
      i++;
    }
    return states[j];
  }

  /**
   * Moves states by 1 and sets current state to null. If you override beginWindow, you must call super.beginWindow(windowId) to ensure
   * proper operator behavior.
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    currentstate++;
    if (currentstate >= getN()) {
      currentstate = 0;
    }
    states[currentstate] = null;
  }

  /**
   * Sets up internal state structure
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    states = new Object[getN()];
    for (int i = 0; i < getN(); i++) {
      states[i] = null;
    }
    currentstate = -1;
  }
}
