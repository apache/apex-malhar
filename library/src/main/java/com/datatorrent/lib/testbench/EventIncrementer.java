/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.testbench;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyValPair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Creates a random movement by taking in a seed stream and incrementing this data. 
 * <p>
 * Takes in a seed stream on port seed and then increments this data on port increment. Data is immediately emitted on output port data.
 * Emits number of tuples on port count<p>
 * The aim is to create a random movement.
 * <br>
 * Examples of application includes<br>
 * random motion<br>
 * <br>
 * <br>
 * <br>
 * <b>Tuple Schema</b>: Each tuple is HashMap<String, ArrayList> on both the ports. Currently other schemas are not supported<br>
 * <b>Port Interface</b><br>
 * <b>seed</b>: The seed data for setting up the incrementer data to work on<br>
 * <b>increment</b>: Small random increments to the seed data. This now creates a randomized change in the seed<br>
 * <b>data</b>: Output of seed + increment<br>
 * <b>count</b>: Emits number of processed tuples per window<br>
 * <br>
 * <b>Properties</b>:
 * <br>keys: In case the value has multiple dimensions. They can be accessed via keys<br>
 * <br>delta: The max value from an increment. The value on increment port is treated as a "percent" of this delta<br>
 * Compile time checks are:<br>
 * <br>
 * <b>Benchmarks</b>: The benchmark was done in local/inline mode<br>
 * Processing tuples on seed port are at 3.5 Million tuples/sec<br>
 * Processing tuples on increment port are at 10 Million tuples/sec<br>
 * <br>
 * @displayName Event Incrementer
 * @category Testbench
 * @tags increment, hashmap
 * @since 0.3.2
 */
public class EventIncrementer extends BaseOperator
{
   /**
   * Input seed port that takes a hashmap of &lt;string,arraylist of integers&gt; which provides seed data for setting up the incrementer data to work on.
   */
  public final transient DefaultInputPort<HashMap<String, ArrayList<Integer>>> seed = new DefaultInputPort<HashMap<String, ArrayList<Integer>>>()
  {
    @Override
    public void process(HashMap<String, ArrayList<Integer>> tuple)
    {
      tuple_count++;
      for (Map.Entry<String, ArrayList<Integer>> e: tuple.entrySet()) {
        if (keys.length != e.getValue().size()) { // bad seed
          return;
          // emit error tuple here
        }
        else {
          ArrayList<KeyValPair<String, Double>> alist = new ArrayList<KeyValPair<String, Double>>(keys.length);
          int j = 0;
          for (Integer s: e.getValue()) {
            KeyValPair<String, Double> d = new KeyValPair<String, Double>(keys[j], new Double(s.doubleValue()));
            alist.add(d);
            j++;
          }
          vmap.put(e.getKey(), alist);
          emitDataTuple(e.getKey(), alist);
        }
      }
    }
  };
  
  /**
   * Input increment port that takes a hashmap of &lt;string,hashmap of &lt;string,number&gt;&gt; which provides small random increments to the seed data.
   */
  public final transient DefaultInputPort<HashMap<String, HashMap<String, Integer>>> increment = new DefaultInputPort<HashMap<String, HashMap<String, Integer>>>()
  {
    @Override
    public void process(HashMap<String, HashMap<String, Integer>> tuple)
    {
      tuple_count++;
      for (Map.Entry<String, HashMap<String, Integer>> e: tuple.entrySet()) {
        String key = e.getKey(); // the key
        ArrayList<KeyValPair<String, Double>> alist = vmap.get(key); // does it have a location?
        if (alist == null) { // if not seeded just ignore
          continue;
        }
        for (Map.Entry<String, Integer> o: e.getValue().entrySet()) {
          String dimension = o.getKey();
          int j = 0;
          int cur_slot = 0;
          int new_slot = 0;
          for (KeyValPair<String, Double> d: alist) {
            if (dimension.equals(d.getKey())) { // Compute the new location
              cur_slot = d.getValue().intValue();
              Double nval = getNextNumber(d.getValue().doubleValue(), delta / 100 * (o.getValue().intValue() % 100), low_limits[j], high_limits[j]);
              new_slot = nval.intValue();
              alist.get(j).setValue(nval);
              break;
            }
            j++;
          }
          if (cur_slot != new_slot) {
            emitDataTuple(key, alist);
          }
        }
      }
    }
  };
  
  /**
   * Output data port that emits a hashmap of &lt;string,string&gt; which is the addition of seed and increment.
   */
  public final transient DefaultOutputPort<HashMap<String, String>> data = new DefaultOutputPort<HashMap<String, String>>();
  
  /**
   * Output count port that emits a hashmap of &lt;string,integer&gt; which contains number of processed tuples per window.
   */
  public final transient DefaultOutputPort<HashMap<String, Integer>> count = new DefaultOutputPort<HashMap<String, Integer>>();
  public static final String OPORT_COUNT_TUPLE_COUNT = "count";
  HashMap<String, ArrayList<KeyValPair<String, Double>>> vmap = new HashMap<String, ArrayList<KeyValPair<String, Double>>>();
  String[] keys = null;
  double[] low_limits = null;
  double[] high_limits = null;
  private double sign = -1.0;
  final double low_limit_default_val = 0;
  final double high_limit_default_val = 100;
  double delta = 1;
  int tuple_count = 0;

  /**
   * The max increment value.
   * @param i
   */
  public void setDelta(double i)
  {
    delta = i;
  }

  public void setKeylimits(ArrayList<String> klist, ArrayList<Double> low, ArrayList<Double> high)
  {
    if (low.size() != high.size()) {
      throw new IllegalArgumentException(String.format("Array sizes for low limits (%d), and high limits (%d) do not match", low.size(), high.size()));
    }
    if (klist.size() != low.size()) {
      throw new IllegalArgumentException(String.format("Array sizes for low limits (%d), does not match number of keys (%d)", low.size(), klist.size()));
    }
    if (low_limits == null) {
      low_limits = new double[low.size()];
    }
    if (high_limits == null) {
      high_limits = new double[high.size()];
    }

    if (keys == null) {
      keys = new String[klist.size()];
    }

    for (int i = 0; i < low.size(); i++) {
      low_limits[i] = low.get(i).doubleValue();
      high_limits[i] = high.get(i).doubleValue();
      keys[i] = klist.get(i);
    }
  }

  public double getNextNumber(double current, double increment, double low, double high)
  {
    double ret = current;
    double range = high - low;
    if (increment > range) { // bad data, do nothing
      ret = current;
    }
    else {
      sign = sign * -1.0;
      ret += sign * increment;
      if (ret < low) {
        ret = (low + high) / 2;
      }
      if (ret > high) {
        ret = (low + high) / 2;
      }
    }
    return ret;
  }

  public void emitDataTuple(String key, ArrayList<KeyValPair<String, Double>> list)
  {
    if (!data.isConnected()) {
      return;
    }
    HashMap<String, String> tuple = new HashMap<String, String>(1);
    String val = new String();
    for (KeyValPair<String, Double> d: list) {
      if (!val.isEmpty()) {
        val += ",";
      }
      Integer ival = d.getValue().intValue();
      val += ival.toString();
    }
    tuple.put(key, val);
    data.emit(tuple);
  }

  @Override
  public void beginWindow(long windowId)
  {
    tuple_count = 0;
  }

  @Override
  public void endWindow()
  {
    if (count.isConnected()) {
      HashMap<String, Integer> tuple = new HashMap<String, Integer>(1);
      tuple.put(OPORT_COUNT_TUPLE_COUNT, new Integer(tuple_count));
      count.emit(tuple);
    }
  }
}
