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
 * Takes a stream of key value pairs via input port "data", and they are ordered by a given key. The ordered tuples are emitted on port "out_data" at the end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, Object><br>
 * <b>out_data</b>: Output data port, emits HashMap<String, Object><br>
 * <b>Properties</b>:
 * <b>key</b>: The key to order by<br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * Parameter "key" cannot be empty<br>
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

  String key = null;
  protected PriorityQueue<Integer> pqueue = null;
  protected HashMap<Integer, ArrayList> smap = null;

 /**
   * The key to order by
   *
   */
  public static final String KEY_KEY = "key";

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
    Integer key;
    while ((key = pqueue.poll()) != null) {
      ArrayList list = smap.get(key);
      for (Object o : list) {
        emit(o);
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
    HashMap<String, Object> tuple = (HashMap<String, Object>) payload;

    Integer val = (Integer) tuple.get(key); // check instanceof?
    if (val == null) {
      // emit error tuple?
      return;
    }
    ArrayList list = smap.get(val);
    if (list == null) { // already in the queue
      list = new ArrayList();
      list.add(tuple);
      smap.put(val, list);
      pqueue.add(val);
    }
    list.add(tuple);
  }

  /**
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)
  {
    boolean ret = true;

    key = config.get(KEY_KEY, "");
    if (key.isEmpty()) {
      ret = false;
      throw new IllegalArgumentException("Parameter \"key\" is empty");
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

    key = config.get(KEY_KEY, "");
    pqueue = new PriorityQueue<Integer>();
    smap = new HashMap<Integer, ArrayList>();
  }
}
