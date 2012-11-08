/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;

/**
 *
 * Occurrences of all values for each key is counted and at the end of window the most frequent value is emitted
 * on output port "count" per key<p>
 * This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects HashMap<String, String><br>
 * <b>most</b>: Output port, emits HashMap<String, HashMap<String, Integer>>(1), where first String is the key, the second String is the value, and Integer is the count of occurrence<br>
 * <br>
 * Properties:<br>
 * none<br>
 * <br>
 * Compile time checks<br>
 * none<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator can process about 30 million unique (k,v immutable) object tuples/sec, and take in a lot more incoming tuples. The operator emits only one tuple per window
 * and hence is not bound by outbound throughput<br>
 * <br>
 * @author amol
 */
public class MostFrequentKeyValue<K, V> extends BaseFrequentKeyValue<K, V>
{
  @OutputPortFieldAnnotation(name = "most")
  public final transient DefaultOutputPort<HashMap<K, HashMap<V, Integer>>> most = new DefaultOutputPort<HashMap<K, HashMap<V, Integer>>>(this);

  @Override
  public boolean compareValue(int val1, int val2)
  {
    return (val1 > val2);
  }

  @Override
  public void emitTuple(HashMap<K, HashMap<V, Integer>> tuple)
  {
    most.emit(tuple);
  }
}
