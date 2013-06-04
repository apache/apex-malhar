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
public class UnifierKeyValRange<K> implements Unifier<KeyValPair<K, HighLow>>
{
  public final transient DefaultOutputPort<KeyValPair<K, HighLow>> mergedport = new DefaultOutputPort<KeyValPair<K, HighLow>>(this);

  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(KeyValPair<K, HighLow> tuple)
  {
    HighLow val = map.get(tuple.getKey());
    if (val == null) {
      val = new HighLow(tuple.getValue().getHigh(), tuple.getValue().getLow());
    }

    if (val.getHigh().doubleValue() < tuple.getValue().getHigh().doubleValue()) {
      val.setHigh(tuple.getValue().getHigh());
    }
    if (val.getLow().doubleValue() > tuple.getValue().getLow().doubleValue()) {
      val.setLow(tuple.getValue().getLow());
    }
  }

  HashMap<K, HighLow> map = new HashMap<K, HighLow>();

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
    if (!map.isEmpty()) {
      for (Map.Entry<K, HighLow> e: map.entrySet()) {
        mergedport.emit(new KeyValPair(e.getKey(), new HighLow(e.getValue().getHigh(), e.getValue().getLow())));
      }
      map.clear();
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
