/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.ArrayList;

/**
 * Takes an ArrayList in stream <b>data</b> and just emits each item in the array. Used for breaking up a ArrayList<p>
 * <br>
 * <br>
 * <b>Port Interface</b><br>
 * <b>data</b>: expects ArrayList<K><br>
 * <b>item</b>: emits K<br>
 * <br>
 * <b>Properties</b>:
 * None
 * <br>
 * <b>Compile time checks are</b>:<br>
 * None
 * <b>Benchmarks</b>
 * TBD
 * <br>
 *
 * @author amol
 */
public class ArraylistToItem<K> extends BaseOperator
{
  public final transient DefaultInputPort<ArrayList<K>> data = new DefaultInputPort<ArrayList<K>>(this)
  {
    @Override
    public void process(ArrayList<K> tuple)
    {
      for (K k: tuple) {
        item.emit(k);
      }
    }
  };
  public final transient DefaultOutputPort<K> item = new DefaultOutputPort<K>(this);
}
