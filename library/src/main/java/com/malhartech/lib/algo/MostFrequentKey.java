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

  HashMap<String, myInteger> keycount = null;

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
              (e.getValue().value > kval)) {
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

  public boolean myValidation(ModuleConfiguration config)
  {
    return true;
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
    keycount = new HashMap<String, myInteger>();
  }
}
