/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableDouble;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.Min;

/**
 *
 * Add all the values for each key on "numerator" and "denominator" and emits quotient at end of window<p>
 * <br>
 * <b>Ports</b>:
 * <b>numerator</b> expects HashMap<K,V extends Number><br>
 * <b>denominator</b> expects HashMap<K,V extends Number><br>
 * <b>quotient</b> emits HashMap<K,Double><br>
 * <br>
 * <br>
 * <b>Compile time checks</b>
 * None<br>
 * <br>
 * <b>Runtime checks</b>
 * None<br>
 * <br>
 * <b>Benchmarks</b><br>
 * <br>
 * Benchmarks:<br>
 * The operator does 20 million tuples/sec as it only emits one per end of window, and is not bounded by outbound I/O<br>
 * <br>
 * <br>
 *
 * @author amol<br>
 *
 */
public class Quotient<K, V extends Number> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "numerator")
  public final transient DefaultInputPort<HashMap<K, V>> numerator = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * Added tuple to the numerator hash
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      addTuple(tuple, numerators);
    }
  };
  @InputPortFieldAnnotation(name = "denominator")
  public final transient DefaultInputPort<HashMap<K, V>> denominator = new DefaultInputPort<HashMap<K, V>>(this)
  {
    /**
     * Added tuple to the denominator hash
     */
    @Override
    public void process(HashMap<K, V> tuple)
    {
      addTuple(tuple, denominators);
    }
  };

  public void addTuple(HashMap<K, V> tuple, HashMap<K, MutableDouble> map)
  {
    for (Map.Entry<K, V> e: tuple.entrySet()) {
      MutableDouble val = map.get(e.getKey());
      if (val == null) {
        val = new MutableDouble(e.getValue().doubleValue());
      }
      else {
        if (countkey) {
          val.value++;
        }
        else {
          val.value += e.getValue().doubleValue();
        }
      }
      map.put(e.getKey(), val);
    }
  }
  @OutputPortFieldAnnotation(name = "quotient")
  public final transient DefaultOutputPort<HashMap<K, Double>> quotient = new DefaultOutputPort<HashMap<K, Double>>(this);
  HashMap<K, MutableDouble> numerators = new HashMap<K, MutableDouble>();
  HashMap<K, MutableDouble> denominators = new HashMap<K, MutableDouble>();

  boolean countkey = false;
  int mult_by = 1;

  @Min(0)
  public int getMult_by()
  {
    return mult_by;
  }

  public boolean getCountkey()
  {
    return countkey;
  }


  public void setMult_by(int i)
  {
    mult_by = i;
  }

  public void setCountkey(boolean i)
  {
    countkey = i;
  }

  /*
   * Clears the cache/hash
   */
  @Override
  public void beginWindow(long windowId)
  {
    numerators.clear();
    denominators.clear();
  }

  /**
   * Generates tuples for each key and emits them. Only keys that are in the denominator are iterated on
   * If the key is only in the numerator, it gets ignored (cannot do divide by 0)
   */
  @Override
  public void endWindow()
  {
    HashMap<K, Double> tuples = new HashMap<K, Double>();
    for (Map.Entry<K, MutableDouble> e: denominators.entrySet()) {
      MutableDouble nval = numerators.get(e.getKey());
      if (nval == null) {
        tuples.put(e.getKey(), new Double(0.0));
      }
      else {
        tuples.put(e.getKey(), new Double((nval.value / e.getValue().value) * mult_by));
      }
    }
    if (!tuples.isEmpty()) {
      quotient.emit(tuples);
    }
  }
}
