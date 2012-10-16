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
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". Occurrences of each key is counted and at the end of window the most frequent key is emitted on output port "count"<p>
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
  @PortAnnotation(name = MostFrequentKey.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = MostFrequentKey.OPORT_COUNT, type = PortAnnotation.PortType.OUTPUT)
})
public class MostFrequentKey extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_COUNT = "count";
  private static Logger LOG = LoggerFactory.getLogger(MostFrequentKey.class);

  HashMap<String, Integer> keylocation = null;

  int[] count = null;
  int current_location = 0;
  final int default_count_size = 1000;
  int current_count_size = default_count_size;

  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    Integer location = keylocation.get((String) payload);
    if (location == null) {
      location = new Integer(current_location);
      current_location++;
      keylocation.put((String) payload, location);
    }
    count[location.intValue()]++;
    if (current_location >= current_count_size) {
      updateCountSize();
    }
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


  @Override
  public void beginWindow()
  {
    // need to periodically clean up the keylocation hash and start again
    // else old keys will fill up the hash, de-silting needs to be done
  }

  @Override
  public void endWindow()
  {
    String key = null;
    int kval = -1;
    for (Map.Entry<String, Integer> e: keylocation.entrySet()) {
      if ((kval == -1) || // first key
              (count[e.getValue().intValue()] > kval)) {
        key = e.getKey();
        kval = count[e.getValue().intValue()];
      }
      count[e.getValue().intValue()] = 0; // clear the positions
    }
    if ((key != null) && (kval > 0)) { // key is null if no
      HashMap<String, Integer> tuple = new HashMap<String, Integer>(1);
      tuple.put(key, new Integer(kval));
      emit(tuple);
    }
    // emit least frequent key here

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
  public boolean checkConfiguration(ModuleConfiguration config)
  {
    boolean ret = true;
    // TBD
    return ret && super.checkConfiguration(config);
  }
}
