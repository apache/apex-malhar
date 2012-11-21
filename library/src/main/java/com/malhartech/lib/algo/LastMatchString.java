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
 * A compare function is operated on a tuple value of type String based on the property "key", "value", and "compare". Every tuple
 * is checked and the last one that passes the condition is send during end of window on port "last". The comparison is done by getting double
 * value from the Number<p>
 * This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects HashMap<K, String><br>
 * <b>last</b>: Output port, emits HashMap<K, String> in end of window for the last tuple on which the compare function is true<br>
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
 * Operator can process > 7 million unique (k,v immutable pairs) tuples/sec, and take in a lot more incoming tuples. The operator emits only one tuple per window
 * and hence is not bound by outbound throughput<br>
 *
 * @author amol
 */
public class LastMatchString<K> extends BaseMatchOperator<K, String>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, String>> data = new DefaultInputPort<HashMap<K, String>>(this)
  {
    /**
     * Processes tuples and keeps a copy of last matched tuple
     */
    @Override
    public void process(HashMap<K, String> tuple)
    {
      String val = tuple.get(getKey());
      if (val == null) {
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
      if (!errortuple) {
        if (compareValue(tvalue)) {
          ltuple = cloneTuple(tuple);
        }
      }
    }
  };
  @OutputPortFieldAnnotation(name = "last")
  public final transient DefaultOutputPort<HashMap<K, String>> last = new DefaultOutputPort<HashMap<K, String>>(this);
  HashMap<K, String> ltuple = null;


  /**
   * Clears cache/hash
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    ltuple = null;
  }

  /**
   * Emits last matching tuple
   */
  @Override
  public void endWindow()
  {
    if (ltuple != null) {
      last.emit(ltuple);
    }
  }
}
