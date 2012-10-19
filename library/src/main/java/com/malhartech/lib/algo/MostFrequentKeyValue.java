/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.GenericNode;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.OperatorConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". Occurrences of all values for each key is counted and at the end of window the most frequent value is emitted
 * on output port "count" per key<p>
 *  This module is an end of window module<br>
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
 * @author amol
 */


@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = MostFrequentKeyValue.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = MostFrequentKeyValue.OPORT_COUNT, type = PortAnnotation.PortType.OUTPUT)
})
public class MostFrequentKeyValue extends GenericNode
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_COUNT = "count";
  private static Logger LOG = LoggerFactory.getLogger(MostFrequentKeyValue.class);

  HashMap<String, HashMap<String, myInteger>> keyvals = null;

  class myInteger {
    int value;
  }
  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    for (Map.Entry<String, String> e: ((HashMap<String, String>) payload).entrySet()) {
      HashMap<String, myInteger> vals = keyvals.get(e.getKey());
      if (vals == null) {
        vals = new HashMap<String, myInteger>();
        keyvals.put(e.getKey(), vals);
      }
      myInteger count = vals.get(e.getValue());
      if (count == null) {
        count = new myInteger();
        vals.put(e.getValue(), count);
      }
      count.value++;
    }
  }

  @Override
  public void beginWindow()
  {
    keyvals.clear();
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<String, HashMap<String, myInteger>> e: keyvals.entrySet()) {
      String val = null;
      int kval = -1;
      HashMap<String, myInteger> vals = e.getValue();
      for (Map.Entry<String, myInteger> v: vals.entrySet()) {
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
        emit(tuple);
      }
    }
  }

  public boolean myValidation(OperatorConfiguration config)
  {
    return true;
  }
   /**
   *
   * @param config
   */
  @Override
  public void setup(OperatorConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("validation failed");
    }
    keyvallocation = new HashMap<String, Integer>();
    keyvals = new HashMap<String, HashMap<String, Integer>>();
    count = new int[default_count_size];
    current_location = 0;
    current_count_size = default_count_size;
    for (int i = 0; i < default_count_size; i++) {
      count[i] = 0;
    }
  }


  /**
   *
   * Checks for user specific configuration values<p>
   *
   * @param config
   * @return boolean
   */
  @Override
  public boolean checkConfiguration(OperatorConfiguration config)
  {
    boolean ret = true;
    // TBD
    return ret && super.checkConfiguration(config);
  }
}
