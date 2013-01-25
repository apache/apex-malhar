/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator.Unifier;
import java.util.ArrayList;


  /**
 *
 * Combiner for an output port that emits object with ArrayList<V>(2) interface and has the processing done
 * with round robin partition. The first element in the ArrayList is high, the next is low
 *
 * @author amol<br>
 *
 */
public class UnifierRange<V extends Number> implements Unifier<ArrayList<V>>
{
  public ArrayList<V> mergedTuple = new ArrayList<V>(2);
  public final transient DefaultOutputPort<ArrayList<V>> mergedport = new DefaultOutputPort<ArrayList<V>>(this);

  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void merge(ArrayList<V> tuple)
  {
    if (mergedTuple.isEmpty()) {
      mergedTuple.addAll(tuple);
    }
    else {
      if (mergedTuple.get(0).doubleValue() < tuple.get(0).doubleValue()) {
        mergedTuple.set(0, tuple.get(0));
      }
      if (mergedTuple.get(1).doubleValue() > tuple.get(1).doubleValue()) {
        mergedTuple.set(1, tuple.get(1));
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    if (!mergedTuple.isEmpty())  {
      mergedport.emit(mergedTuple);
      mergedTuple = new ArrayList<V>(2);
    }  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }
}
