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

/**
 * Not done yet<br>
 * Takes a stream of key value pairs via input port "data". The incoming data is expected to be sorted list. The tuple are then merged into already existing sorted list.
 * At the end of the window the entire sorted list is emitted on output port "sort"<p>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects ArrayList<K><br>
 * <b>sort</b>: Output data port, emits ArrayList<K><br>
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
public class MergeSort<K> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<ArrayList<K>> data = new DefaultInputPort<ArrayList<K>>(this)
  {
    @Override
    public void process(ArrayList<K> tuple)
    {
      mergedList.ensureCapacity(mergedList.size() + tuple.size());
      int lloc = 0;
      for (K o: mergedList) {
        //tbd
        ;
      }
    }
  };
  @OutputPortFieldAnnotation(name = "sort")
  public final transient DefaultOutputPort<ArrayList> sort = new DefaultOutputPort<ArrayList>(this);
  protected ArrayList<K> mergedList = new ArrayList<K>();

  /**
   * Cleanup at the start of window
   */
  @Override
  public void beginWindow(long windowId)
  {
    mergedList = new ArrayList();
  }

  /**
   *
   */
  @Override
  public void endWindow()
  {
    if (mergedList.isEmpty()) {
      return;
    }
    sort.emit(mergedList);
  }
}
