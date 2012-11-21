/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.AbstractBaseNUniqueOperator;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Input stream of key value pairs are ordered by key, and bottom N of the ordered unique tuples per key are emitted on
 * port "top" at the end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<K,V> (<key, value>)<br>
 * <b>top</b>: Output data port, emits HashMap<K, ArrayList<V>> (<key, ArrayList<value>>)<br>
 * <b>Properties</b>:
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * N: Has to be an integer<br>
 * <br>
 * Run time checks are:<br>
 * <br>
 *
 * @author amol<br>
 *
 */
public class BottomNUnique<K, V> extends AbstractBaseNUniqueOperator<K, V>
{
  @OutputPortFieldAnnotation(name = "bottom")
  public final transient DefaultOutputPort<HashMap<K, ArrayList<V>>> bottom = new DefaultOutputPort<HashMap<K, ArrayList<V>>>(this);

  /**
   * Ascending is set to false as we are looking for Bottom N
   * @return false
   */
  @Override
  public boolean isAscending()
  {
    return false;
  }

  /**
   * Emits tuple to port "bottom"
   * @param tuple
   */
  @Override
  public void emit(HashMap<K, ArrayList<V>> tuple)
  {
    bottom.emit(tuple);
  }
}
