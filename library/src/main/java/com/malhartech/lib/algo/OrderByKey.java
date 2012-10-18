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
 * <b>emitcount</b>: If true only the count is emitted instead of the tuples. Default value is false</br>
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
public class OrderByKey extends Module
{
  private static Logger LOG = LoggerFactory.getLogger(OrderByKey.class);
  public static final String IPORT_DATA = "data";
  public static final String OPORT_OUT_DATA = "out_data";

  String key = null;
  final boolean docount_default = false;
  boolean docount = docount_default;
  protected PriorityQueue<Integer> pqueue = null;
  protected HashMap<Integer, Object> smap = null;

 /**
   * The key to order by
   *
   */
  public static final String KEY_KEY = "key";

  /**
   * Emits the count of tuples, instead of the actual tuples at a particular value
   */
  public static final String KEY_DOCOUNT = "docount";

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
      if (docount) {
        HashMap<Integer, Integer> tuple = new HashMap<Integer, Integer>(1);
        tuple.put(key, (Integer) smap.get(key));
        emit(tuple);
      }
      else {
        ArrayList list = (ArrayList) smap.get(key);
        for (Object o: list) {
          emit(o);
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
    HashMap<String, Object> tuple = (HashMap<String, Object>) payload;
    Integer val = (Integer) tuple.get(key); // check instanceof?
    if (val == null) {
      // emit error tuple?
      return;
    }
    if (docount) {
      Integer count = (Integer) smap.get(val);
      if (count == null) {
        count = new Integer(1);
        pqueue.add(val);
      }
      else {
        count = count + 1;
      }
      smap.put(val, count);
    }
    else {
      ArrayList list = (ArrayList) smap.get(val);
      if (list == null) { // already in the queue
        list = new ArrayList();
        list.add(tuple);
        smap.put(val, list);
        pqueue.add(val);
      }
      list.add(tuple);
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

    key = config.get(KEY_KEY, "");
    if (key.isEmpty()) {
      ret = false;
      throw new IllegalArgumentException("Parameter \"key\" is empty");
    }
    return ret;
  }

  public void initializePriorityQueue()
  {
    pqueue = new PriorityQueue<Integer>(5);
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

    key = config.get(KEY_KEY, "");
    docount = config.getBoolean(KEY_DOCOUNT, docount_default);
    initializePriorityQueue();
    smap = new HashMap<Integer, Object>();
  }
}
