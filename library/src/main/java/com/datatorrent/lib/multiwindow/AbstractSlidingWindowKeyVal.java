/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.multiwindow;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.Min;

/**
 *
 * A sliding window class that lets users access past N-1 window states<p>
 * N is a property. The default behavior is just a pass through, i.e. the operator does not do any processing on its own.
 * Users are expected to extend this class and add their specific processing. Users have to define their own output port(s).
 * The tuples are KeyValue pair. This is an abstract class. The concrete class has to provide the interface SlidingWindowObject,
 * which keeps information about each window.<br>
 * This module is end of window as per users choice<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects T (any POJO)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>windowSize i.e. N</b>: Number of windows to keep state on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Benchmark done for a String type with ArrayList of all tuples in that window as the window state. Since the benchmark is only for
 * state save and retrieval, the actual processing is not counted<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractSlidingWindow&lt;T&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>5.6 million windows</b></td><td>tuple is emitted as is</td>
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
 * @author Locknath Shil <locknath@malhar-inc.com><br>
 * <br>
 */
public abstract class AbstractSlidingWindowKeyVal<K, V extends Number, S extends SlidingWindowObject> extends BaseNumberKeyValueOperator<K, V>
{
  /**
   * buffer to hold state information of different windows.
   */
  protected HashMap<K, ArrayList<S>> buffer = new HashMap<K, ArrayList<S>>();
  /**
   * Index of windows stating at 0.
   */
  protected int currentstate = -1;

  /**
   * Concrete class has to implement how they want the tuple to be processed.
   * @param tuple a keyVal pair of tuple.
   */
  public abstract void processDataTuple(KeyValPair<K, V> tuple);

  /**
   * Concrete class has to implement what to emit at the end of window.
   * @param key
   * @param obj
   */
  public abstract void emitTuple(K key, ArrayList<S> obj);

  /**
   * Length of sliding windows. Minimum value is 2.
   */
  @Min(2)
  protected int windowSize = 2;
  protected long windowId;

  /**
   * Getter function for windowSize (number of previous window buffer).
   *
   * @return windowSize
   */
  public int getWindowSize()
  {
    return windowSize;
  }

  /**
   * @param windowSize
   */
  public void setWindowSize(int windowSize)
  {
    this.windowSize = windowSize;
  }
  /**
   * Input port for getting incoming data.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      processDataTuple(tuple);
    }
  };

  /**
   * Moves buffer by 1 and clear contents of current. If you override beginWindow, you must call super.beginWindow(windowId) to ensure
   * proper operator behavior.
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    currentstate++;
    if (currentstate >= windowSize) {
      currentstate = 0;
    }

    for (Map.Entry<K, ArrayList<S>> e: buffer.entrySet()) {
      e.getValue().get(currentstate).clear();
    }
  }

  /**
   * Emit tuple for each key.
   */
  @Override
  public void endWindow()
  {
    for (Map.Entry<K, ArrayList<S>> e: buffer.entrySet()) {
      emitTuple(e.getKey(), e.getValue());
    }
  }
}
