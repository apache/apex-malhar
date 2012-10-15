/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". Occurrences of all values for each key is counted and at the end of window the least frequent value is emitted
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
  @PortAnnotation(name = LeastFrequentKeyValue.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = LeastFrequentKeyValue.OPORT_COUNT, type = PortAnnotation.PortType.OUTPUT)
})
public class LeastFrequentKeyValue extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_COUNT = "count";
  private static Logger LOG = LoggerFactory.getLogger(LeastFrequentKeyValue.class);

  HashMap<String, Integer> keyvallocation = null;
  HashMap<String, HashMap<String, Integer>> keyvals = null;

  int[] count = null;
  int current_location = 0;
  final int default_count_size = 1000;
  int current_count_size = default_count_size;


  /**
   * Keeps the integer location for each key, val pair. This way there is no need to recalculate when the next identical tuple arrives for processing
   * @param key
   * @param value
   * @param loc
   */

  public void insertKeyVal(String key, String value, Integer loc) {
    HashMap<String, Integer> vals = keyvals.get(key);
    if (vals == null) {
      vals = new HashMap<String, Integer>();
    }
    if (vals.get(value) == null) {
      vals.put(value, loc);
    }
    return;
  }

  public void updateCountSize()
  {
    int new_count_size = current_count_size * 2;

    int[] newcount = new int[new_count_size];
    for (int i = 0; i < current_count_size; i++) {
      newcount[i] = count[i];
    }
    for (int i = current_count_size; i < new_count_size; i++) {
      newcount[i] = 0;
    }
    count = newcount;
    current_count_size = new_count_size;
  }


  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    for (Map.Entry<String, String> e: ((HashMap<String, String>)payload).entrySet()) {
      String keyval = e.getKey() + "=" + e.getValue();
      Integer location = keyvallocation.get(keyval);
      if (location == null) {
        location = new Integer(current_location);
        current_location++;
        keyvallocation.put((String)payload, location);
      }
      count[location.intValue()]++;
      insertKeyVal(e.getKey(), e.getValue(), location);
      if (current_location >= current_count_size) {
        updateCountSize();
      }
    }
  }

  @Override
  public void beginWindow()
  {
    // need to periodically clean up the keylocation hash and start again
    // else old keys will fill up the hash, de-silting needs to be done
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<String, HashMap<String, Integer>> e: keyvals.entrySet()) {
      String val = null;
      int kval = -1;
      HashMap<String, Integer> vals = e.getValue();
      for (Map.Entry<String, Integer> v: vals.entrySet()) {
        if ((kval == -1) || // first key
                (count[v.getValue().intValue()] < kval)) {
          val = v.getKey();
          kval = count[v.getValue().intValue()];
        }
        count[v.getValue().intValue()] = 0; // clear the positions
      }
      if ((val != null) && (kval > 0)) { // key is null if no
        HashMap<String, HashMap<String, Integer>> tuple = new HashMap<String, HashMap<String, Integer>>(1);
        HashMap<String, Integer> valpair = new HashMap<String, Integer>(1);
        valpair.put(val, new Integer(kval));
        tuple.put(e.getKey(), valpair);
        emit(tuple);
      }
    }
  }

  public boolean myValidation(ModuleConfiguration config)
  {
    boolean ret = true;
    return ret;
  }
   /**
   *
   * @param config
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
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
  public boolean checkConfiguration(ModuleConfiguration config)
  {
    boolean ret = true;
    // TBD
    return ret && super.checkConfiguration(config);
  }
}
