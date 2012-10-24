/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.OperatorConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a in stream <b>in_data</b> and adds to incoming keys to create a new tuple that is emitted
 * on output port <b>out_data</b>. The aim is to create a load with pair of keys<p>
 * <br>
 * Examples of pairs include<br>
 * publisher,advertizer<br>
 * automobile,model<br>
 * <br>
 * The keys to be inserted are given by the property <b>keys</b>. Users can choose to insert their
 * own values via property <b>values</b>. Insertion can be done as replacement, addition, multiply,
 * or append (append is not yet supported)<br>. For each incoming key users can provide an insertion
 * probability for the insert keys. This allows for randomization of the insert key choice<br><br>
 * <br>
 * Benchmarks: This node has been benchmarked at over 5 million tuples/second in local/inline mode<br>
 *
 * <b>Tuple Schema</b>: Each tuple is HashMap<String, Double> on both the ports. Currently other schemas are not supported<br>
 * <b>Port Interface</b><br>
 * <b>data</b>: emits HashMap<String,Double><br>
 * <b>event</b>: expects HashMap<String,Double><br>
 * <br>
 * <b>Properties</b>:
 * None<br>
 * <br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty<br>
 * <b>values</b> if specified has to be comma separated doubles and their number must match the number of keys<br>
 * <b>weights</b> if specified the format has to be "key1:val1,val2,...,valn;key2:val1,val2,...,valn;...", where n has to be
 * number of keys in parameter <b>keys</b>. If not specified all weights are equal<br>
 * <br>
 *
 * @author amol
 */

public class EventClassifier extends BaseOperator
{
  public final transient DefaultInputPort<HashMap<String, Double>> event = new DefaultInputPort<HashMap<String, Double>>(this)
  {
    @Override
    public void process(HashMap<String, Double> tuple)
    {
    for (Map.Entry<String, Double> e: tuple.entrySet()) {
      String inkey = e.getKey();
      ArrayList<Integer> alist = null;
      if (inkeys != null) {
        alist = inkeys.get(e.getKey());
      }
      if (alist == null) {
        alist = noweight;
      }

      // now alist are the weights
      int rval = random.nextInt(alist.get(alist.size() - 1));
      int j = 0;
      int wval = 0;
      for (Integer ew: alist) {
        wval += ew.intValue();
        if (wval >= rval) {
          break;
        }
        j++;
      }
      HashMap<String, Double> otuple = new HashMap<String, Double>(1);
      String key = wtostr_index.get(j); // the key
      Double keyval = null;
      if (hasvalues) {
        if (voper == value_operation.VOPR_REPLACE) { // replace the incoming value
          keyval = keys.get(key);
        }
        else if (voper == value_operation.VOPR_ADD) {
          keyval = keys.get(key) + e.getValue();
        }
        else if (voper == value_operation.VOPR_MULT) {
          keyval = keys.get(key) * e.getValue();

        }
        else if (voper == value_operation.VOPR_APPEND) { // not supported yet
          keyval = keys.get(key);
        }
      }
      else { // pass on the value from incoming tuple
        keyval = e.getValue();
      }
      otuple.put(key + "," + inkey, keyval);
      data.emit(otuple);
    }
    }
  };
  public final transient DefaultOutputPort<HashMap<String, Double>> data = new DefaultOutputPort<HashMap<String, Double>>(this);
;

  private static Logger LOG = LoggerFactory.getLogger(EventClassifier.class);

  HashMap<String, Double> keys = new HashMap<String, Double>();
  HashMap<Integer, String> wtostr_index = new HashMap<Integer, String>();
  // One of inkeys (Key to weight hash) or noweight (even weight) would be not null
  HashMap<String, ArrayList<Integer>> inkeys = null;
  ArrayList<Integer> noweight = null;
  boolean hasvalues = false;

  int total_weight = 0;
  private Random random = new Random();

  enum value_operation
  {
    VOPR_REPLACE, VOPR_ADD, VOPR_MULT, VOPR_APPEND
  };
  value_operation voper = value_operation.VOPR_REPLACE;


  public void setOperationReplace()
  {
    voper = value_operation.VOPR_REPLACE;
  }

  public void setOperationAdd()
  {
    voper = value_operation.VOPR_ADD;
  }

  public void setOperationMult()
  {
    voper = value_operation.VOPR_MULT;
  }

  public void setOperationAppend()
  {
    voper = value_operation.VOPR_MULT;
  }

   void setKeyWeights(HashMap<String, ArrayList<Integer>> map)
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

   @Override
  public void setup(OperatorConfiguration config)
  {
    noweight = new ArrayList<Integer>();
    for (int i = 0; i < keys.size(); i++) {
      noweight.add(100); // Even distribution
      total_weight += 100;
    }
    noweight.add(total_weight);
  }

  public void setKeyMap(HashMap<String,Double> map)
  {
    int i = 0;
    // First load up the keys and the index hash (wtostr_index) for randomization to work

    for (Map.Entry<String, Double> e: map.entrySet()) {
      keys.put(e.getKey(), e.getValue());
      wtostr_index.put(i, e.getKey());
      i += 1;
    }
  }
}
