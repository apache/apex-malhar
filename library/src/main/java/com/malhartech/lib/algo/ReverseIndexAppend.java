/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.Sink;

/**
 *
 * Takes a stream via input port "data" and emits the reverse index on output port index on end of window<p>
 * <br>
 * Takes in HashMap<Object, Object> and emits HashMap<Object, Object>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<Object, Object>
 * <b>index</b>: Output data port, emits HashMap<Object, ArrayList<Object>>
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
  @PortAnnotation(name = ReverseIndexAppend.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = ReverseIndexAppend.OPORT_INDEX, type = PortAnnotation.PortType.OUTPUT)
})
public class ReverseIndexAppend extends AbstractModule
{
  private static Logger LOG = LoggerFactory.getLogger(ReverseIndexAppend.class);
  public static final String IPORT_DATA = "data";
  public static final String OPORT_INDEX = "index";

  HashMap<Object, ArrayList<Object>> map = null;

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
      ArrayList<Object> list = map.get(e.getValue());
      if (list == null) {
        list = new ArrayList<Object>();
        map.put(e.getValue(), list);
      }
      list.add(e.getKey());
    }
  }

  @Override
  public void beginWindow()
  {
    map.clear();
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<Object, ArrayList<Object>> e: map.entrySet()) {
      HashMap<Object, ArrayList<Object>> tuple = new HashMap<Object, ArrayList<Object>>(1);
      tuple.put(e.getKey(), e.getValue());
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
    map = new HashMap<Object, ArrayList<Object>>();
  }
}
