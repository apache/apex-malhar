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
 * Input stream of key value pairs is ordered by key, and bottom N of the ordered tuples per key are emitted on
 * port "bottom" at the end of window<p>
 * This is an end of window module. At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * The operator assumes that the key, val pairs in the incoming tuple is immutable. If the tuple is mutable users should override makeCopyKey(), and makeCopyValue()
 * methods<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<StriK,V> (<key, value><br>
 * <b>bottom</b>: Output data port, emits HashMap<K, ArrayList<V>> (<key, ArraList<values>>)<br>
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
public class BottomN<K, V> extends BaseNNonUniqueOperator<K,V>
{
  @OutputPortFieldAnnotation(name="bottom")
  public final transient DefaultOutputPort<HashMap<K, ArrayList<V>>> bottom = new DefaultOutputPort<HashMap<K, ArrayList<V>>>(this);

  @Override
  public boolean isAscending()
  {
    return false;
  }

  @Override
  void emit(HashMap<K, ArrayList<V>> tuple)
  {
    bottom.emit(tuple);
  }
}
