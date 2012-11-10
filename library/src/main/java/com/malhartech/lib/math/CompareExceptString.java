/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.algo.MatchString;
import java.util.HashMap;

/**
 *
 * A compare operation is done on String tuple based on the property "key", "value", and "compare" both matching and non matching tuples on emitted on respective ports. If the tuple
 * passed the test, it is emitted on the output port "compare". If the tuple fails it is emitted on port "except". The comparison is done parsing a double
 * value from the String. Both output ports are optional, but at least one has to be connected<p>
 *  * This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: expects HashMap<K,String><br>
 * <b>compare</b>: emits HashMap<K,String> if compare function returns true<br>
 * <b>except</b>: emits HashMap<K.String> if compare function is false<br>
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
 * Operator processes >5 million tuples/sec. The processing is high as it only emits one tuple per window, and is not bound by outbound throughput<br>
 * @author amol
 */
public class CompareExceptString<K> extends MatchString<K,String>
{
  @OutputPortFieldAnnotation(name = "compare", optional=true)
  public final transient DefaultOutputPort<HashMap<K,String>> compare = match;

  @OutputPortFieldAnnotation(name = "except", optional=true)
  public final transient DefaultOutputPort<HashMap<K,String>> except = new DefaultOutputPort<HashMap<K,String>>(this);

  /**
   * Emits if compare port is connected
   * @param tuple
   */
  @Override
  public void tupleMatched(HashMap<K,String> tuple)
  {
    if (compare.isConnected()) {
      compare.emit(cloneTuple(tuple));
    }
  }

  /**
   * Emits if except port is connected
   * @param tuple
   */
  @Override
  public void tupleNotMatched(HashMap<K,String> tuple)
  {
    if (except.isConnected()) {
      except.emit(cloneTuple(tuple));
    }
  }
}
