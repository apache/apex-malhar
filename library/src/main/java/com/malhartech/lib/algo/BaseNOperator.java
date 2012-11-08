/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import java.util.HashMap;
import javax.validation.constraints.Min;

/**
 * Abstract class for basic topN operators; users need to provide processTuple, beginWindow, and endWindow to implement TopN operator<p>
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
abstract public class BaseNOperator<K, V> extends BaseKeyOperator<K>
{
  /**
   * Expects a HashMap<K,V> tuple
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      processTuple(tuple);
    }
  };
  @Min(1)
  int n = 1;

  abstract public void processTuple(HashMap<K, V> tuple);

  @Min(1)
  public void setN(int val)
  {
    n = val;
  }

  public int getN()
  {
    return n;
  }

  public HashMap<K, V> cloneTuple(K key, V val)
  {
    HashMap<K, V> ret = new HashMap<K, V>(1);
    ret.put(cloneKey(key), cloneValue(val));
    return ret;
  }

  public V cloneValue(V v)
  {
    return v;
  }
}
