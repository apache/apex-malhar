/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * Filters incoming stream and emits values as specified by the set of values to filter. If
 * property "inverse" is set to "true", then all keys except those specified by "keys" are emitted. The values are expected to be immutable<p>
 * This operator should not be used with mutable objects. If this operator has immutable Objects, override "cloneCopy" to ensure a new copy is sent out.
 * This is a pass through node<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expect T (a POJO)<br>
 * <b>filter</b>: emits T (a POJO)<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>keys</b>: The keys to pass through. Those not in the list are dropped. A comma separated list of keys<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * <b>keys</b> cannot be empty<br>
 * <br>
 * <b>Specific run time checks</b>: None <br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for FilterValues&lt;T&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 170 million unique T (immutable) tuples/sec (emitted 100 million tuples/sec)</b></td><td>
 * The in-bound throughput and the number of tuples emitted are the main
 * determinant of performance. If the Object is mutable, then the cost of cloning has to factored in, and it may lower the benchmarks</td><tr>
 * </table><br>
 * <p>
 * <b>Function Table (T=String); inverse=false; keys=a,b</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for FilterValues&lt;T&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(T)</th><th><i>filter</i>(T)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>a</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>b</td><td>b</td></tr>
 * <tr><td>Data (process())</td><td>c</td><td></td></tr>
 * <tr><td>Data (process())</td><td>4</td><td></td></tr>
 * <tr><td>Data (process())</td><td>5ah</td><td></td></tr>
 * <tr><td>Data (process())</td><td>h</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>a</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>d</td><td></td></tr>
 * <tr><td>Data (process())</td><td>55</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>5ah</td><td></td></tr>
 * <tr><td>Data (process())</td><td>b</td><td>b</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 *
 */
public class FilterValues<T> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<T> data = new DefaultInputPort<T>(this)
  {
    /**
     * Processes tuple to see if it matches the filter. Emits if at least one key makes the cut
     * By setting inverse as true, match is changed to un-matched
     */
    @Override
    public void process(T tuple)
    {
      boolean contains = values.containsKey(tuple);
      if ((contains && !inverse) || (!contains && inverse)) {
        filter.emit(cloneValue(tuple));
      }
    }
  };
  @OutputPortFieldAnnotation(name = "filter")
  public final transient DefaultOutputPort<T> filter = new DefaultOutputPort<T>(this);
  HashMap<T, Object> values = new HashMap<T, Object>();
  boolean inverse = false;

  /**
   * getter function for parameter inverse
   *
   * @return inverse
   */
  public boolean getInverse()
  {
    return inverse;
  }

  /**
   * True means match; False means unmatched
   * @param val
   */
  public void setInverse(boolean val)
  {
    inverse = val;
  }

  /**
   * Adds a value to the filter list
   *
   * @param val adds to filter list
   */
  public void setValue(T val)
  {
    if (val != null) {
      values.put(val, null);
    }
  }

  /**
   * Adds the list of values to the filter list
   *
   * @param list ArrayList of items to add to filter list
   */
  public void setValues(T[] list)
  {
    if (list != null) {
      for (T e: list) {
        values.put(e, null);
      }
    }
  }

  /**
   * Clears the filter
   */
  public void clearValues()
  {
    values.clear();
  }

  /**
   * Clones V object. By default assumes immutable object (i.e. a copy is not made). If object is mutable, override this method
   *
   * @param val object bo be cloned
   * @return cloned Val
   */
  public T cloneValue(T val)
  {
    return val;
  }
}
