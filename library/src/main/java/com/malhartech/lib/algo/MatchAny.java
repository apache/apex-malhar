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
 * Each tuple is tested for the compare function. The function is given by
 * "key", "value", and "compare". If any tuple passes a Boolean(true) is emitted, else a Boolean(false) is emitted on the output port "any".
 * The comparison is done by getting double value from the Number.<p>
 * This module is a pass through as it emits the moment the condition is met<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects HashMap<K,V extends Number><br>
 * <b>any</b>: Output port, emits Boolean<br>
 * <br>
 * Properties:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>comp<b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * Compile time checks<br>
 * Key must be non empty/null<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes >between 70 to 500 million tuples/sec depending on no match (lower benchmark) and first tuple match (higher benchmark.
 * The processing is high as it only emits one tuple per window, and is not bound by outbound throughput<br>
 * <br>
 * @author amol
 */
public class MatchAny<K, V extends Number> extends BaseMatchOperator<K,V>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      if (result) {
        return;
      }
      V val = tuple.get(getKey());
      if ((val != null) && compareValue(val.doubleValue())) {
        result = true;
        any.emit(true);
      }
    }
  };
  @OutputPortFieldAnnotation(name = "any")
  public final transient DefaultOutputPort<Boolean> any = new DefaultOutputPort<Boolean>(this);
  boolean result = false;

  @Override
  public void beginWindow(long windowId)
  {
    result = false;
  }
}
