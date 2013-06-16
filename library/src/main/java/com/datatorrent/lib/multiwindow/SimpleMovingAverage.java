/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.multiwindow;

import com.datatorrent.lib.util.KeyValPair;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculate simple moving average (SMA) of last N window.
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Expects KeyValPair where K is Object and V is Number.<br>
 * <b>doubleSMA</b>: Emits simple moving average of N window as Double.<br>
 * <b>floatSMA</b>: Emits simple moving average of N window as Float.<br>
 * <b>longSMA</b>: Emits simple moving average of N window as Long.<br>
 * <b>integerSMA</b>: Emits simple moving average of N window as Integer.<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>windowSize</b>: Number of windows to keep state on<br>
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
public class SimpleMovingAverage<K, V extends Number> extends AbstractSlidingWindowKeyVal<K, V, SimpleMovingAverageObject>
{
  private static final Logger logger = LoggerFactory.getLogger(SimpleMovingAverage.class);
  /**
   * Output port to emit simple moving average (SMA) of last N window as Double.
   */
  @OutputPortFieldAnnotation(name = "doubleSMA", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Double>> doubleSMA = new DefaultOutputPort<KeyValPair<K, Double>>(this);
  /**
   * Output port to emit simple moving average (SMA) of last N window as Float.
   */
  @OutputPortFieldAnnotation(name = "floatSMA", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Float>> floatSMA = new DefaultOutputPort<KeyValPair<K, Float>>(this);
  /**
   * Output port to emit simple moving average (SMA) of last N window as Long.
   */
  @OutputPortFieldAnnotation(name = "longSMA", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Long>> longSMA = new DefaultOutputPort<KeyValPair<K, Long>>(this);
  /**
   * Output port to emit simple moving average (SMA) of last N window as Integer.
   */
  @OutputPortFieldAnnotation(name = "integerSMA", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Integer>> integerSMA = new DefaultOutputPort<KeyValPair<K, Integer>>(this);

  /**
   * Create the list if key doesn't exist.
   * Add value to buffer and increment counter.
   * @param tuple
   */
  @Override
  public void processDataTuple(KeyValPair<K, V> tuple)
  {
    K key = tuple.getKey();
    double val = tuple.getValue().doubleValue();
    ArrayList<SimpleMovingAverageObject> dataList = buffer.get(key);

    if (dataList == null) {
      dataList = new ArrayList<SimpleMovingAverageObject>(windowSize);
      for (int i = 0; i < windowSize; ++i) {
        dataList.add(new SimpleMovingAverageObject());
      }
    }

    dataList.get(currentstate).add(val); // add to previous value
    buffer.put(key, dataList);
  }

  /**
   * Calculate average and emit in appropriate port.
   * @param key
   * @param obj
   */
  @Override
  public void emitTuple(K key, ArrayList<SimpleMovingAverageObject> obj)
  {
    double sum = 0;
    int count = 0;
    for (int i = 0; i < windowSize; i++) {
      SimpleMovingAverageObject d = obj.get(i);
      sum += d.getSum();
      count += d.getCount();
    }

    if (count == 0) { // Nothing to emit.
      return;
    }
    if (doubleSMA.isConnected()) {
      doubleSMA.emit(new KeyValPair<K,Double>(key, (sum / count)));
    }
    if (floatSMA.isConnected()) {
      floatSMA.emit(new KeyValPair<K,Float>(key, (float)(sum / count)));
    }
    if (longSMA.isConnected()) {
      longSMA.emit(new KeyValPair<K,Long>(key, (long)(sum / count)));
    }
    if (integerSMA.isConnected()) {
      integerSMA.emit(new KeyValPair<K,Integer>(key, (int)(sum / count)));
    }
  }
}
