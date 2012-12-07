package com.malhartech.lib.algo;

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyOperator;
import java.util.HashMap;

/**
 *
 * Computes and emits distinct tuples of type K (i.e drops duplicates) at end of window<p>
 * <br>
 * This module is same as a "FirstOf" operation on any key, val pair
 * Even though this module produces continuous tuples, at end of window all data is flushed. Thus the data set is windowed
 * and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b><br>
 * <b>data</b>: Input data port expects K<br>
 * <b>distinct</b>: Output data port, emits K<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None <br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for DistinctKey&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 20 Million K,V pairs/s (at 10 million out-bound emits/s)</b></td><td>Emits first instance of an unique k</td><td>In-bound throughput
 * and number of unique k are the main determinant of performance. Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys,
 * the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for DistinctKey&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(K)</th><th><i>distinct</i>(K)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>a</td><td>a</td></tr>
 * <tr><td>Data (process())</td><td>b</td><td>b</td></tr>
 * <tr><td>Data (process())</td><td>c</td><td>c</td></tr>
 * <tr><td>Data (process())</td><td>4</td><td>4</td></tr>
 * <tr><td>Data (process())</td><td>5ah</td><td>5ah</td></tr>
 * <tr><td>Data (process())</td><td>h</td><td>h</td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td></tr>
 * <tr><td>Data (process())</td><td>d</td><td>d</td></tr>
 * <tr><td>Data (process())</td><td>55</td><td>55</td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td></tr>
 * <tr><td>Data (process())</td><td>5ah</td><td></td></tr>
 * <tr><td>Data (process())</td><td>a</td><td></td></tr>
 * <tr><td>Data (process())</td><td>e</td><td>e</td></tr>
 * <tr><td>Data (process())</td><td>1</td><td>1</td></tr>
 * <tr><td>Data (process())</td><td>55</td><td></td></tr>
 * <tr><td>Data (process())</td><td>c</td><td></td></tr>
 * <tr><td>Data (process())</td><td>b</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class DistinctKey<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    /**
     * Process HashMap<K,V> tuple on input port data, and emits if match not found. Updates the cache
     * with new key,val pair
     */
    @Override
    public void process(K tuple)
    {
      if (!map.containsKey(tuple)) {
        distinct.emit(cloneKey(tuple));
        map.put(cloneKey(tuple), null);
      }
    }
  };
  @OutputPortFieldAnnotation(name = "distinct")
  public final transient DefaultOutputPort<K> distinct = new DefaultOutputPort<K>(this);
  protected transient HashMap<K, Object> map = new HashMap<K, Object>();

  /**
   * Clears the cache/hash
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    map.clear();
  }
}
