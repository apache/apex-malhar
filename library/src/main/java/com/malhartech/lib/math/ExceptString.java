/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.algo.Match;
import com.malhartech.lib.algo.MatchString;
import java.util.HashMap;

/**
 *
 * A compare operation is done on tuple sub-classed from Number based on the property "key", "value", and "compare", and not matched tuples are emitted.
 * The comparison is done by parsing double
 * value from the String. Both output ports are optional, but at least one has to be connected<p>
 *  * This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: expects HashMap<K,V><br>
 * <b>compare</b>: emits HashMap<K,V> if compare function returns true<br>
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
 * ExceptString operator processes >8 million tuples/sec. The processing is high as it only emits one tuple per window, and is not bound by outbound throughput<br>
 *
 * @author amol
 */
public class ExceptString<K> extends MatchString<K,String>
{
  @OutputPortFieldAnnotation(name = "except")
  public final transient DefaultOutputPort<HashMap<K,String>> except = new DefaultOutputPort<HashMap<K,String>>(this);

  /**
   * Does nothing. Overrides base as call super.tupleMatched() would emit the tuple
   * @param tuple
   */
  @Override
  public void tupleMatched(HashMap<K,String> tuple)
  {
  }

  /**
   * Emits the tuple. Calls cloneTuple to get a copy, allowing users to override in case objects are mutable
   * @param tuple
   */
  @Override
  public void tupleNotMatched(HashMap<K,String> tuple)
  {
    except.emit(cloneTuple(tuple));
  }
}
