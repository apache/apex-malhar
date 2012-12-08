/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import java.util.HashMap;
import javax.validation.constraints.Min;

/**
 * Abstract class for basic topN operators; users need to provide processTuple, beginWindow, and endWindow to implement TopN operator<p>
 * This is an end of window module. At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>top</b>: emits HashMap&lt;K,ArrayList&lt;V&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * N: Has to be an integer<br>
 * <br>
 * <b>Specific run time checks are</b>: None<br>
 * <br>
 * <b>Benchmark</b>: Not done as this is an abstract operator<br>
 *
 * @author amol<br>
 *
 */
abstract public class AbstractBaseNOperator<K,V> extends BaseKeyValueOperator<K,V>
{
  /**
   * Expects a HashMap<K,V> tuple
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<HashMap<K,V>> data = new DefaultInputPort<HashMap<K,V>>(this)
  {
    @Override
    public void process(HashMap<K,V> tuple)
    {
      processTuple(tuple);
    }
  };
  @Min(1)
  int n = 1;

  /**
   * Implementations should specify exactly how tuples should be processed
   *
   * @param tuple
   */
  abstract public void processTuple(HashMap<K,V> tuple);

  /**
   * Sets value of N (depth)
   * @param val
   */
  public void setN(int val)
  {
    n = val;
  }

  /**
   * getter function for N
   * @return n
   */
  @Min(1)
  public int getN()
  {
    return n;
  }
}
