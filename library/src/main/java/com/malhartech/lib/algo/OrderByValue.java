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
 * Takes a stream of key value pairs via input port "data", and they are ordered by their value. The ordered tuples are emitted on port "out_data" at the end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, Integer><br>
 * <b>out_data</b>: Output data port, emits HashMap<String, Integer><br>
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
  @PortAnnotation(name = OrderByValue.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = OrderByValue.OPORT_OUT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class OrderByValue extends AbstractModule
{
  private static Logger LOG = LoggerFactory.getLogger(OrderByValue.class);
  public static final String IPORT_DATA = "data";
  public static final String OPORT_OUT_DATA = "out_data";

  PriorityQueue<Integer> pqueue = null;
  HashMap<Integer, HashMap<String, Integer>> smap = null;


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
    Integer ival;
    while ((ival = pqueue.poll()) != null) {
      HashMap<String, Integer> istr = smap.get(ival);
      if (istr == null) { // Should always be not null
        continue;
      }
      for (Map.Entry<String, Integer> e: istr.entrySet()) {
        int count = e.getValue().intValue();
        for (int i = 0; i < count; i++) {
          HashMap<String, Integer> tuple = new HashMap<String, Integer>(1);
          tuple.put(e.getKey(), ival);
          emit(tuple);
        }
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
      HashMap<String, Integer> istr = smap.get(e.getValue());
      if (istr == null) { // not in priority queue
        istr = new HashMap<String, Integer>(4);
        istr.put(e.getKey(), new Integer(1));
        smap.put(e.getValue(), istr);
        pqueue.add(e.getValue());
      }
      else { // value is in the priority queue
        Integer scount = istr.get(e.getKey());
        if (scount == null) { // this key does not exist
          istr.put(e.getKey(), new Integer(1));
        }
        else {
          istr.put(e.getKey(), scount + 1);
        }
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
    pqueue = new PriorityQueue<Integer>();
    smap = new HashMap<Integer, HashMap<String, Integer>>();
  }
}
