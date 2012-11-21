/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseMatchOperator;
import java.util.HashMap;

/**
 *
 * All key,val pairs with val of type String are emitted till the first match;  A compare operation is done based on the property "key", "value",
 * and "cmp". Then on no tuple is emitted in that window. The comparison is done by getting double value of the Number.<p>
 * This module is a pass through<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: Input port, expects HashMap&lt;K,String&gt;<br>
 * <b>first</b>: Output port, emits HashMap&lt;K,String&gt; if compare function returns true<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp<b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
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
 * Operator can process > 5 million unique (k,v immutable pairs) tuples/sec, and take in a lot more incoming tuples. The operator emits tuples per key per window
 * till a match is found. So the benchmarks and the outbound I/O would change in runtime<br>
 * <br>
 * @author amol
 */
public class FirstTillMatchString<K> extends BaseMatchOperator<K,String>
{
  @InputPortFieldAnnotation(name="data")
  public final transient DefaultInputPort<HashMap<K, String>> data = new DefaultInputPort<HashMap<K, String>>(this)
  {
    /**
     * Compares the key,val pair with the match condition. Till a match is found tuples are emitted.
     * Once a match is found, state is set to emitted, and no more tuples are compared (no more emits).
     */
    @Override
    public void process(HashMap<K, String> tuple)
    {
      if (emitted) {
        return;
      }
      String val = tuple.get(getKey());
      double tvalue = 0;
      if (val == null) { // skip if key does not exist
        return;
      }
      boolean error = false;
      try {
        tvalue = Double.valueOf(val.toString()).doubleValue();
      }
      catch (NumberFormatException e) {
        error = true;
      }
      if (!error) {
        if (compareValue(tvalue)) {
          emitted = true;
        }
      }
      if (!emitted) {
        first.emit(cloneTuple(tuple));
      }
    }
  };

  @OutputPortFieldAnnotation(name="first")
  public final transient DefaultOutputPort<HashMap<K, String>> first = new DefaultOutputPort<HashMap<K, String>>(this);
  boolean emitted = false;

  /**
   * Emitted set is reset to false
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    emitted = false;
  }
}
