/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableInteger;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in one stream via input port "data". Occurrences of all values for each key is counted and at the end of window the most frequent value is emitted
 * on output port "count" per key<p>
 * This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects HashMap<String, String><br>
 * <b>count</b>: Output port, emits HashMap<String, HashMap<String, Integer>>(1), where first String is the key, the second String is the value, and Integer is the count of occurrence<br>
 * <br>
 * Properties:<br>
 * none<br>
 * <br>
 * Compile time checks<br>
 * none<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * TBD<br>
 *
 * @author amol
 */
public class MostFrequentKeyValue extends BaseOperator
{
  public final transient DefaultInputPort<HashMap<String, String>> data = new DefaultInputPort<HashMap<String, String>>(this)
  {
    @Override
    public void process(HashMap<String, String> tuple)
    {
      for (Map.Entry<String, String> e: tuple.entrySet()) {
        HashMap<String, MutableInteger> vals = keyvals.get(e.getKey());
        if (vals == null) {
          vals = new HashMap<String, MutableInteger>();
          keyvals.put(e.getKey(), vals);
        }
        MutableInteger count = vals.get(e.getValue());
        if (count == null) {
          count = new MutableInteger(0);
          vals.put(e.getValue(), count);
        }
        count.value++;
      }
    }
  };
  public final transient DefaultOutputPort<HashMap<String, HashMap<String, Integer>>> count = new DefaultOutputPort<HashMap<String, HashMap<String, Integer>>>(this);
  HashMap<String, HashMap<String, MutableInteger>> keyvals = new HashMap<String, HashMap<String, MutableInteger>>();

  @Override
  public void beginWindow()
  {
    keyvals.clear();
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<String, HashMap<String, MutableInteger>> e: keyvals.entrySet()) {
      String val = null;
      int kval = -1;
      HashMap<String, MutableInteger> vals = e.getValue();
      for (Map.Entry<String, MutableInteger> v: vals.entrySet()) {
        if ((kval == -1) || // first key
                (v.getValue().value > kval)) {
          val = v.getKey();
          kval = v.getValue().value;
        }
      }
      if ((val != null) && (kval > 0)) { // key should never be null
        HashMap<String, HashMap<String, Integer>> tuple = new HashMap<String, HashMap<String, Integer>>(1);
        HashMap<String, Integer> valpair = new HashMap<String, Integer>(1);
        valpair.put(val, new Integer(kval));
        tuple.put(e.getKey(), valpair);
        count.emit(tuple);
      }
    }
  }
}
