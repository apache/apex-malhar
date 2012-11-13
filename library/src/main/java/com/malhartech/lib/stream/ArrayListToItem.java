/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyOperator;
import java.util.ArrayList;

/**
 * Takes in an ArrayList and emits each item in the array; mainly used for breaking up a ArrayList tuple into Objects<p>
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
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator is able to emit >150 million tuples/sec<br>
 * <br>
 *
 * @author amol
 */
public class ArrayListToItem<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<ArrayList<K>> data = new DefaultInputPort<ArrayList<K>>(this)
  {
    /**
     * Emitting one item at a time
     */
    @Override
    public void process(ArrayList<K> tuple)
    {
      for (K k: tuple) {
        item.emit(cloneKey(k));
      }
    }
  };
  @OutputPortFieldAnnotation(name = "item")
  public final transient DefaultOutputPort<K> item = new DefaultOutputPort<K>(this);
}
