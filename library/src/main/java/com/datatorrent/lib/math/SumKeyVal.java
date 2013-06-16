/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;
import com.datatorrent.lib.util.KeyValPair;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;

/**
 *
 * Emits the sum of values for each key at the end of window. <p> This is an end window operator. Default unifier works as this operator follows sticky partition.<br> <br> <b>Ports</b>:<br>
 * <b>data</b>: expects KeyValPair&lt;K,V extends Number&gt;<br> <b>sum</b>: emits KeyValPair&lt;K,V extends Number&gt;<br> <br> <b>Properties</b>:<br> <b>inverse</b>: If set to true the key in the
 * filter will block tuple<br> <b>filterBy</b>: List of keys to filter on<br>
 * <b>cumulative</b>: boolean flag, if set the sum is not cleared at the end of window, <br>
 * hence generating cumulative sum
 * across streaming windows. Default is false.<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible
 * in inline mode<br> <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Sum&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr> <tr><td><b>20 million tuples/s</b></td>
 * <td>One tuple per key per port</td><td>Mainly dependant on in-bound throughput</td></tr>
 * </table><br> <p> <b>Function Table (K=String, V=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sum&lt;K,V extends Number&gt; operator template"> <tr><th
 * rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th colspan=3>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(KeyValPair&lt;K,V&gt;)</th><th><i>sum</i>(KeyValPair&lt;K,V&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data
 * (process())</td><td>{a=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=20}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{c=1000}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{e=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=46}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=4}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=23}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td>
 * <td>{a=36}<br>{b=37}<br>{c=1000}<br>{d=141}<br>{e=2}</td></tr>
 * </table> <br>
 *
 * @author Amol Kekre (amol@malhar-inc.com)<br> <br>
 */
public class SumKeyVal<K, V extends Number> extends BaseNumberKeyValueOperator<K, V>
{
  protected class SumEntry
  {
    public MutableDouble sum;
    public boolean changed = true;

    SumEntry()
    {
    }

    SumEntry(MutableDouble sum, boolean changed)
    {
      this.sum = sum;
      this.changed = changed;
    }

  }

  /**
   * Input port to receive data.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<KeyValPair<K, V>> data = new DefaultInputPort<KeyValPair<K, V>>(this)
  {
    /**
     * For each tuple (a key value pair) Adds the values for each key.
     */
    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      if (!doprocessKey(key)) {
        return;
      }
      SumEntry val = sums.get(key);
      if (val == null) {
        val = new SumEntry(new MutableDouble(tuple.getValue().doubleValue()), true);
      }
      else {
        val.sum.add(tuple.getValue().doubleValue());
        val.changed = true;
      }
      sums.put(cloneKey(key), val);
      processMetaData(tuple);
    }

    /**
     * Stream codec used for partitioning.
     */
    @Override
    public Class<? extends StreamCodec<KeyValPair<K, V>>> getStreamCodec()
    {
      return getKeyValPairStreamCodec();
    }

  };
  @OutputPortFieldAnnotation(name = "sum", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, V>> sum = new DefaultOutputPort<KeyValPair<K, V>>(this);
  @OutputPortFieldAnnotation(name = "sumDouble", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Double>> sumDouble = new DefaultOutputPort<KeyValPair<K, Double>>(this);
  @OutputPortFieldAnnotation(name = "sumInteger", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Integer>> sumInteger = new DefaultOutputPort<KeyValPair<K, Integer>>(this);
  @OutputPortFieldAnnotation(name = "sumLong", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Long>> sumLong = new DefaultOutputPort<KeyValPair<K, Long>>(this);
  @OutputPortFieldAnnotation(name = "sumShort", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Short>> sumShort = new DefaultOutputPort<KeyValPair<K, Short>>(this);
  @OutputPortFieldAnnotation(name = "sumFloat", optional = true)
  public final transient DefaultOutputPort<KeyValPair<K, Float>> sumFloat = new DefaultOutputPort<KeyValPair<K, Float>>(this);
  protected HashMap<K, SumEntry> sums = new HashMap<K, SumEntry>();
  protected boolean cumulative = false;
  protected boolean emitOnlyWhenChanged = false;

  public boolean isCumulative()
  {
    return cumulative;
  }

  public void setCumulative(boolean cumulative)
  {
    this.cumulative = cumulative;
  }

  public boolean isEmitOnlyWhenChanged()
  {
    return emitOnlyWhenChanged;
  }

  public void setEmitOnlyWhenChanged(boolean emitOnlyWhenChanged)
  {
    this.emitOnlyWhenChanged = emitOnlyWhenChanged;
  }

  /**
   * If you have extended from KeyValPair class and want to do some processing per tuple override this call back.
   *
   * @param tuple
   */
  public void processMetaData(KeyValPair<K, V> tuple)
  {
  }

  /**
   * Emits on all ports that are connected. Data is precomputed during process on input port and endWindow just emits it for each key. Clears the internal data.
   */
  @Override
  @SuppressWarnings("unchecked")
  public void endWindow()
  {
    for (Map.Entry<K, SumEntry> e: sums.entrySet()) {
      K key = e.getKey();
      SumEntry val = e.getValue();
      if (val.changed || !emitOnlyWhenChanged) {
        sum.emit(new KeyValPair<K, V>(key, getValue(val.sum.doubleValue())));
        sumDouble.emit(new KeyValPair<K, Double>(key, val.sum.doubleValue()));
        sumInteger.emit(new KeyValPair<K, Integer>(key, val.sum.intValue()));
        sumFloat.emit(new KeyValPair<K, Float>(key, val.sum.floatValue()));
        sumShort.emit(new KeyValPair<K, Short>(key, val.sum.shortValue()));
        sumLong.emit(new KeyValPair<K, Long>(key, val.sum.longValue()));
      }
    }
    clearCache();
  }

  /**
   * Clears the cache making this operator stateless on window boundary
   */
  public void clearCache()
  {
    if (cumulative) {
      for (Map.Entry<K, SumEntry> e: sums.entrySet()) {
        SumEntry val = e.getValue();
        val.changed = false;
      }
    }
    else {
      sums.clear();
    }
  }

}
