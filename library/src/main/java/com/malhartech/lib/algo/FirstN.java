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

  HashMap<String, myInteger> keycount = null;

  int n_default_value = 1;
  int n = n_default_value;

  class myInteger {
    public myInteger(int i) {value = i;}
    int value;
  }

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
      myInteger count = keycount.get(e.getKey());
      if (count == null) {
        count = new myInteger(0);
        keycount.put(e.getKey(), count);
      }
      count.value++;
      if (count.value <= n) {
        HashMap<String, Object> tuple = new HashMap<String, Object>(1);
        tuple.put(e.getKey(), e.getValue());
        emit(tuple);
      }
    }
  }

  @Override
  public void beginWindow()
  {
    keycount.clear();
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
    keycount = new HashMap<String, myInteger>();
    LOG.debug(String.format("Set up take for %d tuples", n));
  }
}
