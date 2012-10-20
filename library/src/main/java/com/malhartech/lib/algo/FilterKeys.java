/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.GenericNode;
import com.malhartech.api.FailedOperationException;
import com.malhartech.api.OperatorConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes a stream on input port "in_data", and outputs only keys specified by property "keys" on put output port out_data. If
 * property "inverse" is set to "true", then all keys except those specified by "keys" are emitted<p>
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
public class FilterKeys extends GenericNode
{
  private static Logger LOG = LoggerFactory.getLogger(FilterKeys.class);
  public static final String IPORT_IN_DATA = "in_data";
  public static final String OPORT_OUT_DATA = "out_data";

  HashMap<Object, Object> keys = new HashMap<Object, Object>();
  boolean inverse = false;

  /**
   * The group by key
   *
   */
  public static final String KEY_KEYS = "keys";

  /**
   * If inverse is true the filtered keys are dropped and rest are passed through. If inverse if false then the filtered keys are passed through. Default value for "inverse" is "false"<br>
   */
  public static final String KEYS_INVERSE = "inverse";

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
      boolean contains = keys.containsKey(e.getKey());
      if ((contains && !inverse) || (!contains && inverse)) {
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
  public boolean myValidation(OperatorConfiguration config)
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
    // not checking on KEY_INVERSE as it can be any string; only "true" triggers the inverse
    return ret;
  }

  /**
   *
   * @param config
   */
  @Override
  public void setup(OperatorConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }
    String[] klist = config.getTrimmedStrings(KEY_KEYS);
    String istr = config.get(KEYS_INVERSE, "");

    inverse = istr.equals("true");
    String dstr = new String();
    for (String k : klist) {
      keys.put(k, new Object());
      if (!dstr.isEmpty()) {
        dstr += ",";
      }
      dstr += k;
    }
    LOG.debug(String.format("Setting keys \"%s\" and inverse (%s)", dstr, inverse ? "true" : "false"));
  }
}
