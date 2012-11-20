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
 * <b>data</b>: expects HashMap<K,String><br>
 * <b>match</b>: emits HashMap<K,String> if compare function returns true<br>
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
 * Rune time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 7 million tuples/sec (k as immutable). The processing is high as it only emits one tuple per window, and is not bound by outbound throughput<br>
 *
 * @author amol
 */
public class MatchString<K, String> extends BaseMatchOperator<K,String>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, String>> data = new DefaultInputPort<HashMap<K, String>>(this)
  {
    /**
     * Matchs tuple with the value and calls tupleMatched and tupleNotMatched based on if value matches
     */
    @Override
    public void process(HashMap<K, String> tuple)
    {
      String val = tuple.get(getKey());
      if (val == null) { // skip this tuple
        if (emitError) {
          tupleNotMatched(tuple);
        }
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
          tupleMatched(tuple);
        }
        else {
          tupleNotMatched(tuple);
        }
      }
      else if (emitError) {
        tupleNotMatched(tuple);
      }
    }
  };

  @OutputPortFieldAnnotation(name = "match", optional=true)
  public final transient DefaultOutputPort<HashMap<K, String>> match = new DefaultOutputPort<HashMap<K, String>>(this);

  boolean emitError = true;

  /**
   * getter function for emitError flag.<br>
   * Error tuples (no key; val not a number) are emitted if this flag is true. If false they are simply dropped
   * @return emitError
   */
  public boolean getEmitError()
  {
    return emitError;
  }

  /**
   * setter funtion for emitError flag
   * @param val
   */
  public void setEmitError(boolean val)
  {
    emitError = val;
  }

  /**
   * Emits tuple if it. Call cloneTuple to allow users who have mutable objects to make a copy
   * @param tuple
   */
  public void tupleMatched(HashMap<K, String> tuple)
  {
    match.emit(cloneTuple(tuple));
  }

  /**
   * Does not emit tuple, an empty call. Sub class can override
   * @param tuple
   */
  public void tupleNotMatched(HashMap<K, String> tuple)
  {
  }
}
