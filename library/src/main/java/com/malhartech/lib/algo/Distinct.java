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
 * Takes a stream via input port "data" and emits distinct key,val pairs (i.e drops duplicates) on output port "distinct". Restarts at end of window boundary<p>
 * <br>
 * This module is same as a "FirstOf" operation on any key, val pair
 * Even though this module produces continuous tuples, at end of window all data is flushed. Thus the data set is windowed
 * and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, Object>
 * <b>distinct</b>: Output data port, emits HashMap<String, Object>(1)
 * <b>Properties</b>:
 * None
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * None
 * <br>
 * Run time checks are:<br>
 * None as yet
 *
 *
 * @author amol<br>
 *
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = Distinct.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = Distinct.OPORT_DISTINCT, type = PortAnnotation.PortType.OUTPUT)
})
public class Distinct extends AbstractModule
{
  private static Logger LOG = LoggerFactory.getLogger(Distinct.class);
  public static final String IPORT_DATA = "data";
  public static final String OPORT_DISTINCT = "distinct";

  HashMap<Object, HashMap<Object, Object>> mapkeyval = null;


  @Override
  public void beginWindow()
  {
    mapkeyval.clear();
  }

  /**
   *
   * Takes in a key and an arrayIndex. ReverseIndexes the strings in the ArrayIndex
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    for (Map.Entry<String, Object> e: ((HashMap<String, Object>) payload).entrySet()) {
      HashMap<Object, Object> vals =mapkeyval.get(e.getKey());
      if ((vals == null) || !vals.containsKey(e.getValue())) {
        HashMap<String, Object>tuple = new HashMap<String, Object>(1);
        tuple.put(e.getKey(), e.getValue());
        emit(tuple);
        if (vals == null) {
          vals = new HashMap<Object, Object>();
          mapkeyval.put(e.getKey(), vals);
        }
        vals.put(e.getValue(), null);
      }
    }
  }

  /**
   *
   * @param config
   * @return boolean
   */
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
      throw new FailedOperationException("Did not pass validation");
    }
    mapkeyval = new HashMap<Object, HashMap<Object, Object>>();
  }
}
