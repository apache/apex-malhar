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
 * Takes in one stream via input port "data". Occurrences of each key is counted and at the end of window the least frequent key is emitted on output port "count"<p>
 *  This module is an end of window module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects String<br>
 * <b>count</b>: Output port, emits HashMap<String, Integer>(1); where String is the least frequent key, and Integer is the number of its occurances in the window<br>
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
  @PortAnnotation(name = LeastFrequentKey.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = LeastFrequentKey.OPORT_COUNT, type = PortAnnotation.PortType.OUTPUT)
})
public class LeastFrequentKey extends GenericNode
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_COUNT = "count";
  private static Logger LOG = LoggerFactory.getLogger(LeastFrequentKey.class);

  HashMap<String, myInteger> keycount = null;

  class myInteger
  {
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
    myInteger count = keycount.get((String) payload);
    if (count == null) {
      count = new myInteger();
      keycount.put((String) payload, count);
    }
    count.value++;
  }

  @Override
  public void beginWindow()
  {
    keycount.clear();
  }

  @Override
  public void endWindow()
  {
    String key = null;
    int kval = -1;
    for (Map.Entry<String, myInteger> e: keycount.entrySet()) {
      if ((kval == -1) || // first key
              (e.getValue().value < kval)) {
        key = e.getKey();
        kval = e.getValue().value;
      }
      e.getValue().value = 0; // clear the positions
    }
    if ((key != null) && (kval > 0)) { // key is null if no
      HashMap<String, Integer> tuple = new HashMap<String, Integer>(1);
      tuple.put(key, new Integer(kval));
      emit(tuple);
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
    keylocation = new HashMap<String, Integer>();
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
