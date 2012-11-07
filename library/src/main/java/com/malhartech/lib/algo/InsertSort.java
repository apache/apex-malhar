/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.PriorityQueue;

/**
 * <b>Not done yet</b><br>
 * Takes a stream of key value pairs via input port "data". The incoming is merged into already existing sorted list.
 * At the end of the window the entire sorted list is emitted on output port "sort"<p>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: expects ArrayList<K><br>
 * <b>sort</b>: emits ArrayList<K> at the end of window<br>
 * <b>Properties</b>:
 * None<br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * None<br>
 * <br>
 * Run time checks are:<br>
 * None<br>
 * <br>
 *
 * @author amol<br>
 *
 */
public class InsertSort<K> extends BaseOperator
{
  /**
   * Input port that takes in an array of Objects to insert
   */

  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<ArrayList<K>> data = new DefaultInputPort<ArrayList<K>>(this)
  {
    @Override
    public void process(ArrayList<K> tuple)
    {
      // Need to optimzie by insert sorting into an ArrayList
      // and then just emitting the ArrayList as is
      for (K o: tuple) {
        pqueue.add(o);
      }
    }
  };

  @OutputPortFieldAnnotation(name = "sort")
  public final transient DefaultOutputPort<ArrayList<K>> sort = new DefaultOutputPort<ArrayList<K>>(this);
  protected PriorityQueue<K> pqueue = new PriorityQueue<K>();

  /**
   * Cleanup at the start of window
   */
  @Override
  public void beginWindow(long windowId)
  {
    pqueue.clear();
  }

  /**
   * Emit sorted tuple at end of window
   */
  @Override
  public void endWindow()
  {
    if (pqueue.isEmpty()) {
      return;
    }
    ArrayList tuple = new ArrayList();
    Object o;
    while ((o = pqueue.poll()) != null) {
      tuple.add(o);
    }
    sort.emit(tuple);
  }
}
