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
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * Takes a stream on input port "data", and outputs onlye values as specified by the user on put output port "filter". If
 * property "inverse" is set to "true", then all keys except those specified by "keys" are emitted. The values are expected to be immutable<p>
 * This operator should not be used with mutable objects. If this operator has immutable Objects, override "cloneCopy" to ensure a new copy is sent out<br>
 * <br>
 * This is a pass through node. It takes in an Object and outputs an Object<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, V>
 * <b>filter</b>: Output data port, emits HashMap<String, V>
 * <b>Properties</b>:
 * <b>keys</b>: The keys to pass through, rest are filtered/dropped. A comma separated list of keys<br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty<br>
 * <br>
 * Run time checks are:<br>
 * None
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator can emit about 25 million unique V (immutable) tuples/sec, and take in a lot more incoming tuples. The performance is directly proportional to number
 * of objects emitted. If the Object is mutable, then the cost of cloning has to factored in<br>
 * <br>
 * @author amol<br>
 *
 */
public class FilterValues<T> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<T> data = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      boolean contains = values.containsKey(tuple);
      if ((contains && !inverse) || (!contains && inverse)) {
        filter.emit(cloneValue(tuple));
      }
    }
  };

  @OutputPortFieldAnnotation(name = "filter")
  public final transient DefaultOutputPort<T> filter = new DefaultOutputPort<T>(this);
  HashMap<T, Object> values = new HashMap<T, Object>();
  boolean inverse = false;

  public void setInverse(boolean val)
  {
    inverse = val;
  }

  public void setValue(T val)
  {
    if (val != null) {
      values.put(val, null);
    }
  }

  public void setValues(ArrayList<T> vals)
  {
    for (T e : vals) {
      values.put(e, null);
    }
  }

  public void clearValues()
  {
    values.clear();
  }

  public T cloneValue(T val)
  {
    return val;
  }
}
