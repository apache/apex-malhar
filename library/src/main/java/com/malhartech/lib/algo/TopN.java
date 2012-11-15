/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.TopNSort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Orders tuples per key and emits top N tuples per key on end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, Object><br>
 * <b>top</b>: Output data port, emits HashMap<String, Object><br>
 * <b>Properties</b>:
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * N: Has to be an integer<br>
 * <br>
 * Run time checks are:<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator sorts  > 8 million tuples/sec for N = 5. The sorting was done by adding 5 million values. Since only N tuples are emitted in endWindow
 * the operator is not output I/O bound and performance is proportional to N, and not to number of tuples<br>
 * @author amol<br>
 *
 */
public class TopN<K, V> extends BaseNNonUniqueOperator<K,V>
{
  @OutputPortFieldAnnotation(name="top")
  public final transient DefaultOutputPort<HashMap<K, ArrayList<V>>> top = new DefaultOutputPort<HashMap<K, ArrayList<V>>>(this);

  /**
   * returns true
   * @return true
   */
  @Override
  public boolean isAscending()
  {
    return true;
  }

  /**
   * Emits tuple on port "top"
   */
  @Override
  void emit(HashMap<K, ArrayList<V>> tuple)
  {
    top.emit(tuple);
  }
}
