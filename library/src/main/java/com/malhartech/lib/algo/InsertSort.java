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
import com.malhartech.lib.util.MutableInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import javax.validation.constraints.Min;

/**
 * <b>Not done yet optimally</b>. Takes a stream of key value pairs via input port "data". The incoming tuple is merged into already existing sorted list.
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
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator can process > 3Million/tuples with over 1 million unique immutable tuples/sec, and take in a lot more incoming tuples. All tuples
 * are emitted per window. The above benchmark would change if N is larger<br>
 * <br>
 *
 * @author amol<br>
 *
 */
//
// TODO: Override PriorityQueue and rewrite addAll to insert with location
//
public class InsertSort<K> extends AbstractBaseSortOperator<K>
{
  /**
   * Input port that takes in one tuple at a time
   */
  @InputPortFieldAnnotation(name = "data", optional = true)
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    /**
     * Adds tuple to sorted queue
     */
    @Override
    public void process(K tuple)
    {
      processTuple(tuple);
    }
  };
  /**
   * Input port that takes in an array of Objects to insert
   */
  @InputPortFieldAnnotation(name = "datalist", optional = true)
  public final transient DefaultInputPort<ArrayList<K>> datalist = new DefaultInputPort<ArrayList<K>>(this)
  {
    /**
     * Adds tuples to sorted queue
     */
    @Override
    public void process(ArrayList<K> tuple)
    {
      processTuple(tuple);
    }
  };

  @OutputPortFieldAnnotation(name = "sort", optional = true)
  public final transient DefaultOutputPort<ArrayList<K>> sort = new DefaultOutputPort<ArrayList<K>>(this);
  @OutputPortFieldAnnotation(name = "sorthash", optional = true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> sorthash = new DefaultOutputPort<HashMap<K, Integer>>(this);



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
}
