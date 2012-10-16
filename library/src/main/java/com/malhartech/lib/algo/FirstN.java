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
 * Takes in one stream via input port "data". Takes the first N tuples of a particular key and emits them as they come in on output port "first".<p>
 * This module is a pass through module<br>
 * <br>
 * Ports:<br>
 * <b>data</b>: Input port, expects HashMap<String, Object><br>
 * <b>first</b>: Output port, emits HashMap<String, Object><br>
 * <br>
 * Properties:<br>
 * <b>n</b>: Number of tuples to pass through for each key. Default value of N is 1.<br>
 * <br>
 * Compile time checks<br>
 * N if specified must be an integer<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 *
 * @author amol
 */


@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = FirstN.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = FirstN.OPORT_FIRST, type = PortAnnotation.PortType.OUTPUT)
})
public class FirstN extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_FIRST = "first";
  private static Logger LOG = LoggerFactory.getLogger(FirstN.class);

  HashMap<String, Integer> keyloc = null;

  int n_default_value = 1;
  int n = n_default_value;

  final int count_size_default = 1000;
  int count_size = count_size_default;
  int[] count = null;
  int current_loc = 0;

  /**
   * The number of tuples to emit for each key
   *
   */
  public static final String KEY_N = "n";


  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  @SuppressWarnings("ManualArrayToCollectionCopy")
  public void process(Object payload)
  {
    for (Map.Entry<String, Object> e: ((HashMap<String, Object>)payload).entrySet()) {
      Integer loc = keyloc.get(e.getKey());
      if (loc == null) {
        loc = new Integer(current_loc++);
        keyloc.put(e.getKey(), loc);
      }
      int i = loc.intValue();
      count[i]++;
      if (count[i] <= n) {
        HashMap<String, Object> tuple = new HashMap<String, Object>(1);
        tuple.put(e.getKey(), e.getValue());
        emit(tuple);
      }
      if (current_loc >= count_size) {
        updateCountSize();
      }
    }
  }

    public void updateCountSize()
  {
    int new_count_size = count_size * 2;
    int[] newcount = new int[new_count_size];
    for (int i = 0; i < count_size; i++) {
      newcount[i] = count[i];
    }
    for (int i = count_size; i < new_count_size; i++) {
      newcount[i] = 0;
    }
    count = newcount;
    count_size = new_count_size;
  }

  @Override
  public void beginWindow()
  {
    for (int i = 0; i < current_loc; i++) {
      count[i] = 0;
    }
    keyloc.clear();
    current_loc = 0;
  }

  public boolean myValidation(ModuleConfiguration config)
  {
    boolean ret = true;

    String nstr = config.get(KEY_N, "");

    try {
      int value = Integer.parseInt(nstr);
    }
    catch (NumberFormatException e) {
      ret = false;
      throw new IllegalArgumentException(String.format("Property \"%s\" is not a valid integer", KEY_N, nstr));
    }
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
    n = config.getInt(KEY_N, n_default_value);
    LOG.debug(String.format("Set up take for %d tuples", n));
    count = new int[count_size];
    current_loc = 0;
    for (int i = 0; i < count_size; i++) {
      count[i] = 0;
    }
    keyloc = new HashMap<String, Integer>();
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
