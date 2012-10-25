/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableDouble;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in one stream via input port "data". At end of window sends minimum of all values
 * for each key and emits them on port "min"<p>
 * <br>
 * <b>Ports</b>:
 * <b>data</b> expects HashMap<K,V extends Number><br>
 * <b>min</b> emits HashMap<K,V>, one entry per key<br>
 * <br>
 * <b>Compile time checks</b>:
 * None<br>
 * <b>Run time checks</b>:
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Min operator processes >15 million tuples/sec. The processing is high as it only emits one tuple per window, and is not bound by outbound throughput<br>
 *<br>
 * @author amol
 */
public class Min<K, V extends Number> extends BaseNumberOperator<V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        if (e.getValue() == null) {
          continue;
        }
        MutableDouble val = low.get(e.getKey());
        if (val == null) {
          val = new MutableDouble(e.getValue().doubleValue());
          low.put(e.getKey(), val);
        }
        if (val.value > e.getValue().doubleValue()) {
          val.value = e.getValue().doubleValue();
        }
      }
    }
  };
  @OutputPortFieldAnnotation(name = "min")
  public final transient DefaultOutputPort<HashMap<K,V>> min = new DefaultOutputPort<HashMap<K,V>>(this);
  HashMap<K,MutableDouble> low = new HashMap<K,MutableDouble>();

  @Override
  public void beginWindow()
  {
    low.clear();
  }

  /**
   * Node only works in windowed mode. Emits all data upon end of window tuple
   */
  @Override
  public void endWindow()
  {
    if (!low.isEmpty()) {
      HashMap<K, V> tuple = new HashMap<K, V>(low.size());
      for (Map.Entry<K,MutableDouble> e: low.entrySet()) {
        tuple.put(e.getKey(), getValue(e.getValue().value));
      }
      min.emit(tuple);
    }
  }
}
