/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.HighLow;
import java.util.ArrayList;

/**
 *
 * A division operation is done on consecutive tuples on ports numerator and denominator. The operator is idempotent as the division is done
 * in order, i.e. the first number on denominator port would divide the first number on the numerator port.<p>
 * This is a pass through operator<br>
 * <p>
 * <b>Ports</b>:<br>
 * <b>numerator</b>: expects Number<br>
 * <b>denominator</b>: expects Number<br>
 * <b>longQuotient</b>: emits Long<br>
 * <b>integerQuotient</b>: emits Integer<br>
 * <b>doubleQuotient</b>: emits Double<br>
 * <b>floatQuotient</b>: emits Float<br>
 * <b>longRemainder</b>: emits Long<br>
 * <b>integerRemainder</b>: emits Integer<br>
 * <b>doubleRemainder</b>: emits Double<br>
 * <b>floatRemainder</b>: emits Float<br>
 * <b>errordata</b>: emits String<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <b>Compile time checks</b>: None<br>
 * <b>Run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Division operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 8 Million pairs/s</b></td><td>1 tuple per incoming tuple per connected port</td><td>In-bound rate and network i/o</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Division operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th colspan=2>In-bound (process)</th><th colspan=9>Out-bound (emit)</th></tr>
 * <tr><th><i>numerator</i></th><th><i>denominator</i></th>
 * <th><i>longQuotient</i></th><th><i>integerQuotient</i></th><th><i>doubleQuotient</i></th><th><i>floatQuotient</i></th>
 * <th><i>longRemainder</i></th><th><i>integerRemainder</i></th><th><i>doubleRemainder</i></th><th><i>floatRemainder</i></th>
 * <th><i>errordata</i></th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td>
 * <td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td>
 * <td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td>
 * <td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>11</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>5</td><td>2</td><td>2</td><td>2.2</td><td>2.2</td><td>1</td><td>1</td><td>1.0</td><td>1.0</td><td></td></tr>
 * <tr><td>Data (process())</td><td>12</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>15</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>16</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>5</td><td>2</td><td>2</td><td>2.3</td><td>2.3</td><td>2</td><td>2</td><td>2.0</td><td>2.0</td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>6</td><td>2</td><td>2</td><td>2.5</td><td>2.5</td><td>3</td><td>3</td><td>3.0</td><td>3.0</td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>8</td><td>2</td><td>2</td><td>2.0</td><td>2.0</td><td>0</td><td>0</td><td>0.0</td><td>0.0</td><td></td></tr>
 * <tr><td>Data (process())</td><td></td><td>0</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td>"Error(0.0)"</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td>
 * <td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td>
 * <td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td>
 * <td>N/A</td></tr>
 * </table>
 * <br>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Division extends BaseOperator
{
  public final transient DefaultInputPort<Number> numerator = new DefaultInputPort<Number>(this)
  {
    @Override
    public void process(Number tuple)
    {
      numer.add(tuple);
      if (denom.size() > index) {
        int loc = denom.size();
        if (loc > numer.size()) {
          loc = numer.size();
        }
        emit(numer.get(loc-1), denom.get(loc-1));
        index++;
      }
    }
  };

  public final transient DefaultInputPort<Number> denominator = new DefaultInputPort<Number>(this)
  {
    @Override
    public void process(Number tuple)
    {
      if (tuple.doubleValue() == 0.0) {
        errordata.emit("Error(0.0)");
        return;
      }
      denom.add(tuple);
      if (numer.size() > index) {
        int loc = denom.size();
        if (loc > numer.size()) {
          loc = numer.size();
        }
        emit(numer.get(loc-1), denom.get(loc-1));
        index++;
      }
    }
  };

  @OutputPortFieldAnnotation(name = "longQuotient", optional = true)
  public final transient DefaultOutputPort<Long> longQuotient = new DefaultOutputPort<Long>(this);

  @OutputPortFieldAnnotation(name = "integerQuotient", optional = true)
  public final transient DefaultOutputPort<Integer> integerQuotient = new DefaultOutputPort<Integer>(this);

  @OutputPortFieldAnnotation(name = "doubleQuotient", optional = true)
  public final transient DefaultOutputPort<Double> doubleQuotient = new DefaultOutputPort<Double>(this);

  @OutputPortFieldAnnotation(name = "floatQuotient", optional = true)
  public final transient DefaultOutputPort<Float> floatQuotient = new DefaultOutputPort<Float>(this);

  @OutputPortFieldAnnotation(name = "longRemainder", optional = true)
  public final transient DefaultOutputPort<Long> longRemainder = new DefaultOutputPort<Long>(this);

  @OutputPortFieldAnnotation(name = "integerRemainder", optional = true)
  public final transient DefaultOutputPort<Integer> integerRemainder = new DefaultOutputPort<Integer>(this);

  @OutputPortFieldAnnotation(name = "doubleRemainder", optional = true)
  public final transient DefaultOutputPort<Double> doubleRemainder = new DefaultOutputPort<Double>(this);

  @OutputPortFieldAnnotation(name = "floatRemainder", optional = true)
  public final transient DefaultOutputPort<Float> floatRemainder = new DefaultOutputPort<Float>(this);

  @OutputPortFieldAnnotation(name = "errorData", error = true)
  public final transient DefaultOutputPort<String> errordata = new DefaultOutputPort<String>(this);


  public void emit(Number numer, Number denom)
  {
    Long lQuotient = null;
    Double dQuotient = null;
    Long lRemainder = null;
    Double dRemainder = null;

    if (longQuotient.isConnected()) {
      longQuotient.emit(lQuotient = numer.longValue() / denom.longValue());
    }

    if (longRemainder.isConnected()) {
      longRemainder.emit(lRemainder = numer.longValue() % denom.longValue());
    }

    if (integerQuotient.isConnected()) {
      integerQuotient.emit(lQuotient == null ? (int)(numer.longValue() % denom.longValue()) : lQuotient.intValue());
    }

    if (integerRemainder.isConnected()) {
      integerRemainder.emit(lRemainder == null ? (int)(numer.longValue() % denom.longValue()) : lRemainder.intValue());
    }

    if (doubleQuotient.isConnected()) {
      doubleQuotient.emit(dQuotient = numer.doubleValue() / denom.doubleValue());
    }

    if (doubleRemainder.isConnected()) {
      doubleRemainder.emit(dRemainder = numer.doubleValue() % denom.doubleValue());
    }

    if (floatQuotient.isConnected()) {
      floatQuotient.emit(dQuotient == null ? (float)(numer.doubleValue() / denom.doubleValue()) : dQuotient.floatValue());
    }

    if (floatRemainder.isConnected()) {
      floatRemainder.emit(dRemainder == null ? (float)(numer.doubleValue() % denom.doubleValue()) : dRemainder.floatValue());
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    numer.clear();
    denom.clear();
    index = 0;
  }

  private ArrayList<Number> numer = new ArrayList<Number>();
  private ArrayList<Number> denom = new ArrayList<Number>();
  private int index = 0;
}
