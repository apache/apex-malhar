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
 * Takes a stream on input port "in_data", and outputs only keys specified by property "keys" on put output port out_data<p>
 * <br>
 * This is a pass through node. It takes in HashMap<String, Object> and outputs HashMap<String, Object><br>
 * <br>
 * <b>Ports</b>
 * <b>in_data</b>: Input data port expects HashMap<String, Object>
 * <b>out_data</b>: Output data port, emits HashMap<String, Object>
 * <b>Properties</b>:
 * <b>keys</b>: The keys to pass through, rest are filtered/dropped. A comma separated list of keys<br>
 *
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty<br>
 * <br>
 * Run time checks are:<br>
 * None
 * <br>
 *
 * @author amol<br>
 *
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = FilterKeys.IPORT_IN_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = FilterKeys.OPORT_OUT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class FilterKeys extends AbstractModule
{
  private static Logger LOG = LoggerFactory.getLogger(FilterKeys.class);
  public static final String IPORT_IN_DATA = "in_data";
  public static final String OPORT_OUT_DATA = "out_data";

  HashMap<Object, Object> keys = new HashMap<Object, Object>();

  /**
   * The group by key
   *
   */
  public static final String KEY_KEYS = "keys";

  /**
   *
   * Takes in a key and an arrayIndex. ReverseIndexes the strings in the ArrayIndex
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    HashMap<String, Object> tuple = null;
    for (Map.Entry<String, Object> e: ((HashMap<String, Object>) payload).entrySet()) {
      if (keys.containsKey(e.getKey())) {
        if (tuple == null) {
          tuple = new HashMap<String, Object>(4); // usually the filter keys are very few, so 4 is just fine
        }
        tuple.put(e.getKey(), e.getValue());
      }
    }
    if (tuple != null) {
      emit(tuple);
    }
  }

  /**
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)
  {
    boolean ret = true;
    String[] kstr = config.getTrimmedStrings(KEY_KEYS);

    if (kstr.length == 0) {
      ret = false;
      throw new IllegalArgumentException("Parameter \"keys\" is empty");
    }
    else {
      LOG.debug(String.format("Number of keys are %d", kstr.length));
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
      throw new FailedOperationException("Did not pass validation");
    }
    String[] klist = config.getTrimmedStrings(KEY_KEYS);

    for (String k : klist) {
      keys.put(k, new Object());
    }
  }
}
