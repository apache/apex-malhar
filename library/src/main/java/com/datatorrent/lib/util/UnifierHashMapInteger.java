/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * Combiner for an output port that emits object with Map<K,Integer> interface and has the processing done
 * with round robin partitions. The combiner needs to add values of a key from every partition
 *
 * @author amol<br>
 *
 */
public class UnifierHashMapInteger<K> implements Unifier<HashMap<K, Integer>>
{
  public HashMap<K, Integer> mergedTuple = new HashMap<K, Integer>();
  public final transient DefaultOutputPort<HashMap<K, Integer>> mergedport = new DefaultOutputPort<HashMap<K, Integer>>(this);

  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(HashMap<K, Integer> tuple)
  {
    for (Map.Entry<K, Integer> e: tuple.entrySet()) {
      Integer val = mergedTuple.get(e.getKey());
      if (val == null) {
        val = e.getValue();
      }
      else {
        val += e.getValue();
      }
      mergedTuple.put(e.getKey(), val);
    }
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
      mergedTuple = new HashMap<K, Integer>();
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
