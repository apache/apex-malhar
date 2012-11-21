/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.util.BaseMatchOperator;
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
 * <b>data</b>: Input port, expects HashMap<K,String><br>
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
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes >between 12 to 500 million tuples/sec depending on all match (lower benchmark) and no match (higher benchmark.
 * The processing is high as it only emits one tuple per window, and is not bound by outbound throughput<br>
 * <br>
 *
 * @author amol
 */
public class MatchAnyString<K> extends BaseMatchOperator<K, String>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, String>> data = new DefaultInputPort<HashMap<K, String>>(this)
  {
    /**
     * Emits true on first matching tuple
     */
    @Override
    public void process(HashMap<K, String> tuple)
    {
      if (result) {
        return;
      }
      String val = tuple.get(getKey());
      if (val == null) { // skip if key does not exist
        return;
      }
      double tvalue = 0;
      boolean errortuple = false;
      try {
        tvalue = Double.parseDouble(val.toString());
      }
      catch (NumberFormatException e) {
        errortuple = true;
      }
      result = !errortuple && compareValue(tvalue);
      if (result) {
        any.emit(true);
      }
    }
  };
  @OutputPortFieldAnnotation(name = "any")
  public final transient DefaultOutputPort<Boolean> any = new DefaultOutputPort<Boolean>(this);
  boolean result = false;

  /**
   * Resets match flag
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    result = false;
  }
}
