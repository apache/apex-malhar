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
 * Takes in two streams via input ports "numerator" and "denominator". At the
 * end of window computes the margin for each key and emits the result on port
 * "margin" (1 - numerator/denominator).<p> <br> Each stream is added to a hash.
 * The values are added for each key within the window and for each stream.<<br>
 * This node only functions in a windowed stram application<br> <br> Compile
 * time error processing is done on configuration parameters<br>
 * <b>Ports</b>:
 * <b>numerator</b> expects HashMap<K,V><br>
 * <b>denominator</b> expects HashMap<K,V><br>
 * <b>margin</b> emits HashMap<K,Double>, one entry per key<br>
 * <br>
 * <b>Compile time checks</b><br>
 * None<br>
 * <br> Run time error processing are emitted on _error port. The errors
 * are:<br> Divide by zero (Error): no result is emitted on "outport".<br> Input
 * tuple not an integer on denominator stream: This tuple would not be counted
 * towards the result.<br> Input tuple not an integer on numerator stream: This
 * tuple would not be counted towards the result.<br> <br>
 * <b>Benchmarks</b><br>
 * Margin operator processes >40 million tuples/sec. The processing is high as it only emits one tuple per window, and is not bound by outbound throughput<br>
 * <br>
 *
 * @author amol<br>
 *
 */
public class Margin<K, V extends Number> extends BaseNumberOperator<V>
{
  @InputPortFieldAnnotation(name = "numerator")
  public final transient DefaultInputPort<HashMap<K, V>> numerator = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      addTuple(tuple, numerators);
    }
  };
  @InputPortFieldAnnotation(name = "denominator")
  public final transient DefaultInputPort<HashMap<K, V>> denominator = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      addTuple(tuple, denominators);
    }
  };

  public void addTuple(HashMap<K, V> tuple, HashMap<K, MutableDouble> map)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      MutableDouble val = map.get(e.getKey());
      if (val == null) {
        val = new MutableDouble(0.0);
        map.put(e.getKey(), val);
      }
      val.value += e.getValue().doubleValue();
    }
  }
  @OutputPortFieldAnnotation(name = "margin")
  public final transient DefaultOutputPort<HashMap<K, V>> margin = new DefaultOutputPort<HashMap<K, V>>(this);
  HashMap<K, MutableDouble> numerators = new HashMap<K, MutableDouble>();
  HashMap<K, MutableDouble> denominators = new HashMap<K, MutableDouble>();
  boolean percent = false;

  public void setPercent(boolean val)
  {
    percent = val;
  }

  @Override
  public void beginWindow()
  {
    numerators.clear();
    denominators.clear();
  }

  @Override
  public void endWindow()
  {
    HashMap<K, V> tuples = new HashMap<K, V>();
    Double val;
    for (Map.Entry<K, MutableDouble> e: denominators.entrySet()) {
      MutableDouble nval = numerators.get(e.getKey());
      if (nval == null) {
        nval = new MutableDouble(0.0);
      }
      else {
        numerators.remove(e.getKey()); // so that all left over keys can be reported
      }
      if (percent) {
        val = 1 - nval.value / e.getValue().value * 100;
      }
      else {
        val = 1 - nval.value / e.getValue().value;
      }
      tuples.put(e.getKey(), getValue(val.doubleValue()));
    }
    if (!tuples.isEmpty()) {
      margin.emit(tuples);
    }
  }
}
