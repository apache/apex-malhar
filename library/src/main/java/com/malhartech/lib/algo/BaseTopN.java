/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.TopNSort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract class for basic topN operators. Need to provide insertIntoQueue, beginWindow, and endWindow to implement TopN operator<p>
 * port "top" at the end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: expects HashMap<K, V><br>
 * <b>top</b>: emits HashMap<K, ArrayList<V>><br>
 * <b>Properties</b>:
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * N: Has to be an integer<br>
 * <br>
 * Run time checks are:<br>
 * None
 * <br>
 *
 * @author amol<br>
 *
 */
abstract public class BaseTopN<K, V> extends BaseOperator
{
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        insertIntoQueue(e.getKey(), e.getValue());
      }
    }
  };

  abstract public void insertIntoQueue(K k, V v);

  public final transient DefaultOutputPort<HashMap<K, ArrayList<V>>> top = new DefaultOutputPort<HashMap<K, ArrayList<V>>>(this);
  final int default_n_value = 5;
  int n = default_n_value;

  public void setN(int val)
  {
    n = val;
  }
}
