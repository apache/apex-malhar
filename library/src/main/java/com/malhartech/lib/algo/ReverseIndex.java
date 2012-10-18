/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.Module;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.OperatorConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes a stream via input port "data" and emits the reverse index on output port index<p>
 * <br>
 * Takes in HashMap<Object, Object> and emits HashMap<Object, Object>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<Object, Object>
 * <b>index</b>: Output data port, emits HashMap<Object, Object>
 * <b>Properties</b>:
 *
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <br>
 * Run time checks are:<br>
 *
 *
 * @author amol<br>
 *
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = ReverseIndex.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = ReverseIndex.OPORT_INDEX, type = PortAnnotation.PortType.OUTPUT)
})
public class ReverseIndex extends Module
{
  private static Logger LOG = LoggerFactory.getLogger(ReverseIndex.class);
  public static final String IPORT_DATA = "data";
  public static final String OPORT_INDEX = "index";


  /**
   *
   * Takes in a key and an arrayIndex. ReverseIndexes the strings in the ArrayIndex
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    for (Map.Entry<Object, Object> e: ((HashMap<Object, Object>)payload).entrySet()) {
      HashMap<Object, Object> tuple = new HashMap<Object, Object>(1);
      tuple.put(e.getValue(), e.getKey());
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
  }
}
