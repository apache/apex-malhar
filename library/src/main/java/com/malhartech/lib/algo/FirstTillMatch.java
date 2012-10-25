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
 * Takes in one stream via input port "data". A compare function is imposed based on the property "key", "value", and "compare". All tuples
 * are emitted till a tuple matches this test. Then on no tuple is emitted in that window. The comparison is done by getting double value from the Number.<p>
 * This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects HashMap<K,V><br>
 * <b>first</b>: Output port, emits HashMap<K,V> if compare function returns true<br>
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
 * Integer: ?? million tuples/s<br>
 * Double: ?? million tuples/s<br>
 * Long: ?? million tuples/s<br>
 * Short: ?? million tuples/s<br>
 * Float: ?? million tuples/s<br>
 *
 * @author amol
 */
public class FirstTillMatch<K, V extends Number> extends BaseMatchOperator<K,V>
{
  @InputPortFieldAnnotation(name="data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      if (emitted) {
        return;
      }
      V val = tuple.get(getKey());
      if (val == null) { // skip if the key does not exist
        return;
      }
      if (compareValue(val.doubleValue())) {
        emitted = true;
      }
      if (!emitted) {
        first.emit(cloneTuple(tuple));
      }
    }
  };

  @OutputPortFieldAnnotation(name="first")
  public final transient DefaultOutputPort<HashMap<K, V>> first = new DefaultOutputPort<HashMap<K, V>>(this);
  boolean emitted = false;

  @Override
  public void beginWindow()
  {
    emitted = false;
  }
}
