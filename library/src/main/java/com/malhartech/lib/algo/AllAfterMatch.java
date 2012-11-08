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
 * A compare operation is done on input tuple based on the property "key", "value", and "compare". All tuples
 * are emitted (inclusive) once a match is made. The comparison is done by getting double value from the Number.<p>
 * This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects HashMap<K, V extends Number><br>
 * <b>allafter</b>: Output port, emits HashMap<K, V extends Number> if compare function returns true<br>
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
 * Operator processes >10 million tuples/sec. It is a clone and emit once a tuple matches<br>
 *<br>
 * @author amol
 */
public class AllAfterMatch<K, V extends Number> extends BaseMatchOperator<K,V>
{
  @InputPortFieldAnnotation(name="data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      if (doemit) {
        allafter.emit(cloneTuple(tuple));
        return;
      }
      V v = tuple.get(getKey());
      if (v == null) { // error tuple
        return;
      }
      if (compareValue(v.doubleValue())) {
        doemit = true;
        allafter.emit(cloneTuple(tuple));
      }
    }
  };

  @OutputPortFieldAnnotation(name="allafter")
  public final transient DefaultOutputPort<HashMap<K, V>> allafter = new DefaultOutputPort<HashMap<K, V>>(this);
  boolean doemit = false;

  @Override
  public void beginWindow(long windowId)
  {
    doemit = false;
  }
}
