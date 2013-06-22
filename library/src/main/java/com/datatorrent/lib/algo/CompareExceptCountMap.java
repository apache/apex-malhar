/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.UnifierHashMap;
import com.datatorrent.lib.util.UnifierSumNumber;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * A count is done on how many tuples of value type Number satisfy the compare function. The function is given by
 * "key", "value", and "cmp". If a tuple passed the test count is incremented. On end of window count is emitted on the output port "count".
 * The comparison is done by getting double value from the Number.<p>
 * This module is an end of window module. If no tuple comes in during a window 0 is emitted on both ports, thus no matter what one Integer
 * tuple is emitted on each port<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,V extends Number&gt;<br>
 * <b>count</b>: emits Integer<br>
 * <b>except</b>: emits Integer<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp</b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * <b>Specific compile time checks</b>:<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for CompareExceptCountMap&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 100 Million K,V pairs/s</b></td><td>All tuples are processed and only one Integer is emitted per window per port</td>
 * <td>In-bound is the main determinant of performance. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String,V=Integer); key=a; value=3; cmp=eq</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for CompareExceptCountMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(Map&lt;K,V&gt;)</th><th><i>count</i>(Integer)</th><th><i>except</i>(Integer)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=5,b=5}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3,h=20,c=2}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=3}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46,e=2}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4}</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=23}</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>2</td><td>9</td></tr>
 * <tr><td>Begin Window (begindWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>0</td><td>1</td></tr>
 * <tr><td>Begin Window (begindWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>0</td><td>0</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class CompareExceptCountMap<K, V extends Number> extends MatchMap<K, V>
{
  @OutputPortFieldAnnotation(name = "count")
  public final transient DefaultOutputPort<Integer> count = new DefaultOutputPort<Integer>()
  {
    @Override
    public Unifier<Integer> getUnifier()
    {
      return new UnifierSumNumber();
    }
  };


  @OutputPortFieldAnnotation(name = "except")
  public final transient DefaultOutputPort<Integer> except = new DefaultOutputPort<Integer>()
  {
    @Override
    public Unifier<Integer> getUnifier()
    {
      return new UnifierSumNumber();
    }
  };


  protected int tcount = 0;
  protected int icount = 0;

  /**
   * Increments matched tuple count
   * @param tuple
   */
  @Override
  public void tupleMatched(Map<K, V> tuple)
  {
    tcount++;
  }

  /**
   * Increments not-matched tuple count
   * @param tuple
   */
  @Override
  public void tupleNotMatched(Map<K, V> tuple)
  {
    icount++;
  }

  /**
   * Emits the counts
   */
  @Override
  public void endWindow()
  {
    count.emit(new Integer(tcount));
    except.emit(new Integer(icount));
    tcount = 0;
    icount = 0;
  }
}
