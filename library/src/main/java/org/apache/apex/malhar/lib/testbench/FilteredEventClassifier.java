/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.testbench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This operator takes in a stream of tuples
 * and randomly emits them based on the specified total_filter and pass_filter values.&nbsp;
 * Emitted tuples are modified based on the specified key map and key weights.
 * <p>
 * The aim is to create another stream representing a subsection of incoming load<p>
 * <br>
 * Examples of pairs include<br>
 * publisher,advertizer<br>
 * automobile,model<br>
 * <br>
 * The keys to be inserted are given by the property <b>keys</b>. Users can choose to insert their
 * own values via property <b>values</b>.<br>
 * For each incoming key users can provide an insertion
 * probability for the insert keys. This allows for randomization of the insert key choice<br><br>
 * <br>
 * <b>Tuple Schema</b>: Each tuple is HashMap<String, Double> on both the ports. Currently other schemas are not supported<br>
 * <b>Port Interface</b><br>
 * <b>out_data</b>: Output port for emitting the new tuple<br>
 * <b>in_data</b>: Input port for receiving the incoming tuple<br>
 * <br>
 * <b>Properties</b>:
 * <b>keys</b> is a comma separated list of keys. This key are the insert keys in the tuple<br>
 * <b>values</b> are comma separated list of values. This value is for insertion into the <value> field in the tuple. also called "insert value". If not specified the incoming values are not changed<br>
 * <b>weights</b> are comma separated list of probability weights for each incoming key. For each incoming key the weights have to be provided. If this parameter is empty all the weights are even for all keys<br>
 * <b>percent<b>A number between 0 and 100. This is the percent of the time a new tuple is created. If say the number is 1, then a randomly selected 1 tuple out of 100 would create an output typle<br>
 * <br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty<br>
 * <b>values</b> if not provided the incoming value is passed through<br>
 * <b>weights</b> if specified the format has to be "key1:val1,val2,...,valn;key2:val1,val2,...,valn;...", where n has to be
 * number of keys in parameter <b>keys</b>. If not specified all weights are equal<br>
 * <b>filter</b> The first number has to be less than the second and both have to be positive<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * This node has been benchmarked at over 20 million tuples/second in local/inline mode<br>
 * </p>
 * @displayName Filtered Event Classifier
 * @category Test Bench
 * @tags filter
 * @since 0.3.2
 */
public class FilteredEventClassifier<T> extends BaseOperator
{
  /**
   * The input port on which tuples are received.
   */
  public final transient DefaultInputPort<HashMap<String, T>> data = new DefaultInputPort<HashMap<String, T>>()
  {
    @Override
    public void process(HashMap<String, T> tuple)
    {
      int fval = random.nextInt(total_filter);
      if (fval >= pass_filter) {
        return;
      }

      // Now insertion needs to be done
      for (Map.Entry<String, T> e: tuple.entrySet()) {
        String[] twokeys = e.getKey().split(",");
        if (twokeys.length == 2) {
          String inkey = twokeys[1];
          ArrayList<Integer> alist = noweight;
          if (inkeys != null) {
            alist = inkeys.get(inkey);
          }
          // now alist are the weights
          int rval = random.nextInt(alist.get(alist.size() - 1));
          int j = 0;
          int wval = 0;
          for (Integer ew: alist) {
            wval += ew.intValue();
            if (wval > rval) {
              break;
            }
            j++;
          }
          HashMap<String, T> otuple = new HashMap<String, T>(1);
          String key = wtostr_index.get(j); // the key
          T keyval;
          if (hasvalues) {
            keyval = keys.get(key);
          } else { // pass on the value from incoming tuple
            keyval = e.getValue();
          }
          otuple.put(key + "," + inkey, keyval);
          filter.emit(otuple);
        }
      }
    }
  };

  /**
   * The output port which emits filtered and modified tuples.
   */
  public final transient DefaultOutputPort<HashMap<String, T>> filter = new DefaultOutputPort<HashMap<String, T>>();
  HashMap<String, T> keys = new HashMap<String, T>();
  HashMap<Integer, String> wtostr_index = new HashMap<Integer, String>();
  // One of inkeys (Key to weight hash) or noweight (even weight) would be not null
  HashMap<String, ArrayList<Integer>> inkeys = null;
  ArrayList<Integer> noweight = null;
  boolean hasvalues = false;
  int total_weight = 0;
  int pass_filter = 0;
  int total_filter = 0;
  private Random random = new Random();

  public void setPassFilter(int i)
  {
    pass_filter = i;
  }

  public void setTotalFilter(int i)
  {
    total_filter = i;
  }

  public void setKeyMap(HashMap<String, T> map)
  {
    int i = 0;
    // First load up the keys and the index hash (wtostr_index) for randomization to work
    boolean foundvalue = false;
    for (Map.Entry<String, T> e: map.entrySet()) {
      keys.put(e.getKey(), e.getValue());
      foundvalue = foundvalue || (e.getValue() != null);
      wtostr_index.put(i, e.getKey());
      i += 1;
    }
    hasvalues = foundvalue;
  }

  @Override
  public void setup(OperatorContext context)
  {
    noweight = new ArrayList<Integer>();
    for (int i = 0; i < keys.size(); i++) {
      noweight.add(100); // Even distribution
      total_weight += 100;
    }
    noweight.add(total_weight);
    if (pass_filter > total_filter) {
      throw new IllegalArgumentException(String.format("Pass filter (%d) cannot be >= Total filter (%d)", pass_filter, total_filter));
    }
  }

  public void setKeyWeights(HashMap<String, ArrayList<Integer>> map)
  {
    if (inkeys == null) {
      inkeys = new HashMap<String, ArrayList<Integer>>();
    }
    for (Map.Entry<String, ArrayList<Integer>> e: map.entrySet()) {
      inkeys.put(e.getKey(), e.getValue());
    }

    for (Map.Entry<String, ArrayList<Integer>> e: inkeys.entrySet()) {
      ArrayList<Integer> list = e.getValue();
      int total = 0;
      for (Integer i: list) {
        total += i.intValue();
      }
      list.add(total);
    }
  }
}
