/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.util.AbstractBaseSortOperator;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;
import java.util.HashMap;

/**
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
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator can process > 3Million/tuples with over 1 million unique immutable tuples/sec, and take in a lot more incoming tuples. All tuples
 * are emitted per window. The above benchmark would change if N is larger<br>
 * <br>
 * @author amol<br>
 *
 */
public class MergeSort<K> extends AbstractBaseSortOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<ArrayList<K>> data = new DefaultInputPort<ArrayList<K>>(this)
  {
    @Override
    public void process(ArrayList<K> tuple)
    {
      processTuple(tuple);
    }
  };
  @OutputPortFieldAnnotation(name = "sort")
  public final transient DefaultOutputPort<ArrayList<K>> sort = new DefaultOutputPort<ArrayList<K>>(this);
  @OutputPortFieldAnnotation(name = "sorthash", optional = true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> sorthash = new DefaultOutputPort<HashMap<K, Integer>>(this);

  /*
   * <b>Currently implemented with individual keys inserted. Needs to be reimplemented</b>
   *
   */
  @Override
  public void processTuple(ArrayList<K> tuple)
  {
    super.processTuple(tuple);
  }


  @Override
  public boolean doEmitList()
  {
    return sort.isConnected();
  }

  @Override
  public boolean doEmitHash()
  {
    return sorthash.isConnected();
  }

  @Override
  public void emitToList(ArrayList<K> list)
  {
    sort.emit(list);
  }

  @Override
  public void emitToHash(HashMap<K,Integer> map)
  {
    sorthash.emit(map);
  }
}
