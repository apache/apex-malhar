/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator.Unifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Combiner for an output port that emits object with Map<K,ArrayList<V>(2)> interface and has the processing done
 * with round robin partition. The first element in the ArrayList is high, the next is low
 *
 * @author amol<br>
 *
 */
public class UnifierHashMapRange<K, V extends Number> implements Unifier<HashMap<K, ArrayList<V>>>
{
  public HashMap<K, ArrayList<V>> mergedTuple = new HashMap<K, ArrayList<V>>();
  public final transient DefaultOutputPort<HashMap<K, ArrayList<V>>> mergedport = new DefaultOutputPort<HashMap<K, ArrayList<V>>>(this);

  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void merge(HashMap<K, ArrayList<V>> tuple)
  {
    for (Map.Entry<K, ArrayList<V>> e: tuple.entrySet()) {
      ArrayList<V> val = mergedTuple.get(e.getKey());
      if (val == null) {
        val = new ArrayList<V>(2);
        val.addAll(e.getValue());
        mergedTuple.put(e.getKey(), val);
      }
      else {
        ArrayList<V> newval = null;
        if (val.get(0).doubleValue() < e.getValue().get(0).doubleValue()) {
          val.set(0,e.getValue().get(0));
        }
        if (val.get(1).doubleValue() > e.getValue().get(1).doubleValue()) {
          val.set(1,e.getValue().get(1));
        }
      }
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
      mergedTuple = new HashMap<K, ArrayList<V>>();
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
