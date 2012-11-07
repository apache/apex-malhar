/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;

/**
 *
 * Takes in one stream via input port "data". A count is done on how many tuples satisfy the compare function. The function is given by
 * "key", "value", and "compare". If a tuple passed the test count is incremented. On end of window count is emitted on the output port "count".
 * The comparison is done by getting double value from the Number.<p>
 *  This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: expects HashMap<K,V><br>
 * <b>count</b>: emits Integer<br>
 * <br>
 * Properties:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>compare<b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
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
 * Operator processes >16 million tuples/sec. The processing is high as it only emits one tuple per window, and is not bound by outbound throughput<br>
 * @author amol
 */

public class CompareCountString<K> extends MatchString<K,String>
{
  @OutputPortFieldAnnotation(name="count")
  public final transient DefaultOutputPort<Integer> count = new DefaultOutputPort<Integer>(this);
  @OutputPortFieldAnnotation(name="except")
  public final transient DefaultOutputPort<Integer> except = new DefaultOutputPort<Integer>(this);

  private int tcount = 0;
  private int icount = 0;

  @Override
  public void tupleMatched(HashMap<K, String> tuple)
  {
    tcount++;
  }

  @Override
  public void tupleNotMatched(HashMap<K, String> tuple)
  {
    icount++;
  }

  @Override
  public void beginWindow(long windowId)
  {
     tcount = 0;
     icount = 0;
  }

  @Override
  public void endWindow()
  {
    count.emit(tcount);
    except.emit(icount);
  }
}
