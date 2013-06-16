/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator.Unifier;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Combiner for an output port that emits object with Map<K,V> interface and has the processing done
 * with round robin partition for a Sum operation, i.e. key,vals pairs need to be combined back together
 *
 * @author amol<br>
 *
 */
public class UnifierHashMapSumKeys<K, V extends Number> extends BaseNumberKeyValueOperator<K,V> implements Unifier<HashMap<K, V>>
{
  public HashMap<K, Double> mergedTuple = new HashMap<K, Double>();
  public final transient DefaultOutputPort<HashMap<K, V>> mergedport = new DefaultOutputPort<HashMap<K, V>>(this);

  @Override
  public void process(HashMap<K, V> tuple)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      Double val = mergedTuple.get(e.getKey());
      if (val == null) {
        mergedTuple.put(e.getKey(), e.getValue().doubleValue());
      }
      else {
        val += e.getValue().doubleValue();
        mergedTuple.put(e.getKey(), val);
      }
    }
  }

  /**
   * emits mergedTuple on mergedport if it is not empty
   */
  @Override
  public void endWindow()
  {
    if (!mergedTuple.isEmpty()) {
      HashMap<K, V> stuples = new HashMap<K, V>();
      for (Map.Entry<K, Double> e: mergedTuple.entrySet()) {
        stuples.put(e.getKey(), getValue(e.getValue()));
      }
      mergedport.emit(stuples);
      mergedTuple = new HashMap<K, Double>();
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
