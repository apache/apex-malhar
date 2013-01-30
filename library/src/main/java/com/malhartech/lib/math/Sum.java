/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.lib.util.BaseNumberValueOperator;
import com.malhartech.lib.util.UnifierSumNumber;

/**
 *
 * Emits the sum of values at the end of window. <p>
 * This is an end of window operator<br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>sum</b>: emits V extends Number<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>cumulative</b>: boolean flag, if set the sum is not cleared at the end of window, <br>
 *    hence generating cumulative sum across streaming windows. Default is false.<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Sum&lt;V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 500 Million tuples/s</b></td><td>One Integer tuple per window per port</td><td>In-bound rate is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sum&lt;V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(V)</th><th><i>sum</i>(V)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>1000</td><td></td></tr>
 * <tr><td>Data (process())</td><td>10</td><td></td></tr>
 * <tr><td>Data (process())</td><td>52</td><td></td></tr>
 * <tr><td>Data (process())</td><td>22</td><td></td></tr>
 * <tr><td>Data (process())</td><td>14</td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>4</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>1106</td></tr>
 * </table>
 * <br>
 *
 * @param <V>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class Sum<V extends Number> extends BaseNumberValueOperator<V> implements Unifier<V>
{
  /**
   * Input port to receive data.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>(this)
  {
    /**
     * Computes sum and count with each tuple
     */
    @Override
    public void process(V tuple)
    {
      merge(tuple);
      tupleAvailable = true;
    }
  };

  @Override
  public void merge(V tuple)
  {
    sums += tuple.doubleValue();
    tupleAvailable = true; // also need to set here for Unifier
  }
  @OutputPortFieldAnnotation(name = "sum", optional = true)
  public final transient DefaultOutputPort<V> sum = new DefaultOutputPort<V>(this)
  {
    @Override
    public Unifier<V> getUnifier()
    { // UnifierSumNumber
      return Sum.this;
    }
  };
  @OutputPortFieldAnnotation(name = "sumDouble", optional = true)
  public final transient DefaultOutputPort<Double> sumDouble = new DefaultOutputPort<Double>(this)
  {
    @Override
    public Unifier<Double> getUnifier()
    {
      UnifierSumNumber ret = new UnifierSumNumber<Double>();
      ret.setType(Double.class);
      return ret;
    }
  };
  @OutputPortFieldAnnotation(name = "sumInteger", optional = true)
  public final transient DefaultOutputPort<Integer> sumInteger = new DefaultOutputPort<Integer>(this)
  {
    @Override
    public Unifier<Integer> getUnifier()
    {
      UnifierSumNumber ret = new UnifierSumNumber<Integer>();
      ret.setType(Integer.class);
      return ret;
    }
  };
  @OutputPortFieldAnnotation(name = "sumLong", optional = true)
  public final transient DefaultOutputPort<Long> sumLong = new DefaultOutputPort<Long>(this)
  {
    @Override
    public Unifier<Long> getUnifier()
    {
      UnifierSumNumber ret = new UnifierSumNumber<Long>();
      ret.setType(Long.class);
      return ret;
    }
  };
  @OutputPortFieldAnnotation(name = "sumShort", optional = true)
  public final transient DefaultOutputPort<Short> sumShort = new DefaultOutputPort<Short>(this)
  {
    @Override
    public Unifier<Short> getUnifier()
    {
      UnifierSumNumber ret = new UnifierSumNumber<Short>();
      ret.setType(Short.class);
      return ret;
    }
  };
  @OutputPortFieldAnnotation(name = "sumFloat", optional = true)
  public final transient DefaultOutputPort<Float> sumFloat = new DefaultOutputPort<Float>(this)
  {
    @Override
    public Unifier<Float> getUnifier()
    {
      UnifierSumNumber ret = new UnifierSumNumber<Float>();
      ret.setType(Float.class);
      return ret;
    }
  };

  protected transient double sums = 0;
  protected transient boolean tupleAvailable = false;
  protected boolean cumulative = false;

  public boolean isCumulative()
  {
    return cumulative;
  }

  public void setCumulative(boolean cumulative)
  {
    this.cumulative = cumulative;
  }

  /**
   * Emits sum and count if ports are connected
   */
  @Override
  public void endWindow()
  {
    if (doSumEmit()) {
      sum.emit(getValue(sums));
      sumDouble.emit(sums);
      sumInteger.emit((int) sums);
      sumLong.emit((long) sums);
      sumShort.emit((short) sums);
      sumFloat.emit((float) sums);
      tupleAvailable = false;
    }
    clearCache();
  }

  /**
   * Clears the cache making this operator stateless on window boundary
   */
  public void clearCache()
  {
    if (!cumulative) {
      sums = 0;
    }
  }

  /**
   * Decides whether emit has to be done in this window on port "sum"
   *
   * @return true is sum port is connected
   */
  public boolean doSumEmit()
  {
    return sum.isConnected() && tupleAvailable;
  }
}
