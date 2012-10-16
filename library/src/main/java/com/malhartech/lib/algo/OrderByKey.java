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
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes a stream of key value pairs via input port "data", and they are ordered by key. The ordered tuples are emitted on port "out_data" at the end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, Object><br>
 * <b>out_data</b>: Output data port, emits HashMap<String, Object><br>
 * <b>Properties</b>:
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <br>
 * Run time checks are:<br>
 * <br>
 *
 * @author amol<br>
 *
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = OrderByKey.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = OrderByKey.OPORT_OUT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class OrderByKey extends AbstractModule
{
  private static Logger LOG = LoggerFactory.getLogger(OrderByKey.class);
  public static final String IPORT_DATA = "data";
  public static final String OPORT_OUT_DATA = "out_data";

  PriorityQueue<String> pqueue = null;
  HashMap<String, ArrayList> smap = null;


  /**
   * Cleanup at the start of window
   */
  @Override
  public void beginWindow()
  {
    pqueue.clear();
    smap.clear();
  }

  /**
   *
   */
  @Override
  public void endWindow()
  {
    String key;
    while ((key = pqueue.poll()) != null) {
      ArrayList list = smap.get(key);
      for (Object o : list) {
        HashMap<String, Object> tuple = new HashMap<String, Object>(1);
        tuple.put(key, o);
        emit(tuple);
      }
    }
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
    for (Map.Entry<String, Integer> e: ((HashMap<String, Integer>) payload).entrySet()) {
      ArrayList list = smap.get(e.getKey());
      if (list == null) { // not in priority queue
        list = new ArrayList(4);
        list.add(e.getValue());
        smap.put(e.getKey(), list);
        pqueue.add(e.getKey());
      }
      else { // value is in the priority queue
        list.add(e.getValue());
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
    pqueue = new PriorityQueue<String>();
    smap = new HashMap<String, ArrayList>();
  }
}
