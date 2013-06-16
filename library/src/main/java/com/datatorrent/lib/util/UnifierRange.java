/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

import java.util.ArrayList;


  /**
 *
 * Combiner for an output port that emits object with ArrayList<V>(2) interface and has the processing done
 * with round robin partition. The first element in the ArrayList is high, the next is low
 *
 * @author amol<br>
 *
 */
public class UnifierRange implements Unifier<HighLow>
{
  public HighLow mergedTuple = null;
  public final transient DefaultOutputPort<HighLow> mergedport = new DefaultOutputPort<HighLow>(this);

  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(HighLow tuple)
  {
    if (mergedTuple == null) {
      mergedTuple = new HighLow(tuple.getHigh(), tuple.getLow());
    }
    else {
      if (mergedTuple.getHigh().doubleValue() < tuple.getHigh().doubleValue()) {
        mergedTuple.setHigh(tuple.getHigh());
      }
      if (mergedTuple.getLow().doubleValue() > tuple.getLow().doubleValue()) {
        mergedTuple.setLow(tuple.getLow());
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
    if (mergedTuple != null) {
      mergedport.emit(mergedTuple);
      mergedTuple = null;
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }
}
