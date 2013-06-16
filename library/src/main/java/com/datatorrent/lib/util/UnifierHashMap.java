/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator.Unifier;
import java.util.HashMap;

/**
 *
 * Combiner for an output port that emits object with Map<K,V> interface and has the processing done
 * with sticky key partition, i.e. each one key belongs only to one partition. The final output of the
 * combiner is a simple merge into a single object that implements Map
 *
 * @author amol<br>
 *
 */
public class UnifierHashMap<K, V> implements Unifier<HashMap<K, V>>
{
  public HashMap<K, V> mergedTuple = new HashMap<K, V>();
  public final transient DefaultOutputPort<HashMap<K, V>> mergedport = new DefaultOutputPort<HashMap<K, V>>(this);

  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * This is a merge operation for operators that use sticky key partition
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(HashMap<K, V> tuple)
  {
    mergedTuple.putAll(tuple);
  }

  /**
   * a no op
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
  }


  /**
   * emits mergedTuple on mergedport if it is not empty
   */
  @Override
  public void endWindow()
  {
    if (!mergedTuple.isEmpty())  {
      mergedport.emit(mergedTuple);
      mergedTuple = new HashMap<K, V>();
    }
  }

  /**
   * a no-op
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /**
   * a noop
   */
  @Override
  public void teardown()
  {
  }
}
