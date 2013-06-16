/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberValueOperator;
import com.datatorrent.lib.util.KeyValPair;
import javax.validation.constraints.Min;

/**
 *
 * Emits the change in the value in stream on port data (as compared to a base value set via port base) if the consecutive value exceeds a threshold. <p>
 * This is a pass through node<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br>
 * <b>alert</b>: emits KeyValPair&lt;K,KeyValPair&lt;V,Double&gt;&gt;(1)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>threshold</b>: The threshold of change between consecutive tuples of the same key that triggers an alert tuple<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for ChangeAlertMap&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 94 million/s K,V pairs were processed and about 2000 K,V pairs/s were emitted</b></td>
 * <td>Emits one K,V pair per alert</td><td>In-bound rate and the number of alerts are the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String, V=Integer); percentThreshold=5</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for ChangeAlertMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(KeyValPair&lt;K,V&gt;)</th><th><i>alert</i>(KeyValPair&lt;K,KeyValPair&lt;V,Double&gt;&gt;(1))</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=200,b=50,c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=201,b=50,c=1001}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=202,b=51,c=1002}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=203,b=51,c=1030}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=204,b=55,c=1050}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=210,b=60,c=1050}</td><td>{b={60,9.09}}</td></tr>
 * <tr><td>Data (process())</td><td>{a=225,b=61,c=1052}</td><td>{a={225,7.1428}}</td></tr>
 * <tr><td>Data (process())</td><td>{a=226,b=63,c=1060}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td></td></tr>
 * </table>
 * <br>
 *
 * @author Locknath Shil <locknath@malhar-inc.com><br>
 * <br>
 */
public class ChangeAlert<V extends Number> extends BaseNumberValueOperator<V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    /**
     * Process each key, compute change or percent, and emit it.
     * If we get 0 as tuple next will be skipped.
     */
    @Override
    public void process(V tuple)
    {
      double tval = tuple.doubleValue();
      if (baseValue == 0){ // Avoid divide by zero, Emit an error tuple?
        baseValue = tval;
        return;
      }
      double change = tval - baseValue;
      double percent = (change / baseValue) * 100;
      if (percent < 0.0) {
        percent = 0.0 - percent;
      }
      if (percent > percentThreshold) {
        KeyValPair<V, Double> kv = new KeyValPair<V, Double>(cloneKey(tuple), percent);
        alert.emit(kv);
      }
      baseValue = tval;
    }
  };
  // Default "pass through" unifier works as tuple is emitted as pass through
  @OutputPortFieldAnnotation(name = "alert")
  public final transient DefaultOutputPort<KeyValPair<V, Double>> alert = new DefaultOutputPort<KeyValPair<V, Double>>(this);
  /**
   * baseValue is a stateful field. It is retained across windows
   */
  private double baseValue = 0;
  @Min(1)
  private double percentThreshold = 0.0;

  /**
   * getter function for threshold value
   *
   * @return threshold value
   */
  @Min(1)
  public double getPercentThreshold()
  {
    return percentThreshold;
  }

  /**
   * setter function for threshold value
   */
  public void setPercentThreshold(double d)
  {
    percentThreshold = d;
  }
}
