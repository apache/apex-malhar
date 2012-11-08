/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;

/**
 *
 * A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "match". The comparison is done by getting double
 * value from the Number. Both output ports are optional, but at least one has to be connected<p>
 *  * This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: expects HashMap<K,V extends Number><br>
 * <b>match</b>: emits HashMap<K,V> if compare function returns true<br>
 * <br>
 * Properties:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>comp<b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * Compile time checks<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes >10 million tuples/sec. The processing is high as it only emits one tuple per window, and is not bound by outbound throughput<br>
 *
 * @author amol
 */
public class Match<K, V extends Number> extends BaseMatchOperator<K, V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      V v = tuple.get(getKey());
      if (v == null) { // skip this tuple
        tupleNotMatched(tuple);
        return;
      }
      if (compareValue(v.doubleValue())) {
        tupleMatched(tuple);
      }
      else {
        tupleNotMatched(tuple);
      }
    }
  };
  @OutputPortFieldAnnotation(name = "match", optional=true)
  public final transient DefaultOutputPort<HashMap<K, V>> match = new DefaultOutputPort<HashMap<K, V>>(this);

  public void tupleMatched(HashMap<K, V> tuple)
  {
    match.emit(cloneTuple(tuple));
  }

  public void tupleNotMatched(HashMap<K, V> tuple)
  {
  }
}
