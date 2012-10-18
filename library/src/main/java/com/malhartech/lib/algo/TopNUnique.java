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
import com.malhartech.lib.util.TopNSort;
import com.malhartech.lib.util.TopNUniqueSort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a stream of key value pairs via input port "data", and they are ordered by key. Top N of the ordered unique tuples per key are emitted on
 * port "top" at the end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, Object> (key, value)<br>
 * <b>top</b>: Output data port, emits HashMap<String, ArrayList<HashMap<Object, Integer>>> (key, <value, Integer>)br>
 * <b>Properties</b>:
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * N: Has to be an integer<br>
 * <br>
 * Run time checks are:<br>
 * <br>
 *
 * @author amol<br>
 *
 */

@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = TopNUnique.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = TopNUnique.OPORT_TOP, type = PortAnnotation.PortType.OUTPUT)
})
public class TopNUnique<E> extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_TOP = "top";
  private static Logger LOG = LoggerFactory.getLogger(TopNUnique.class);

  final String default_n_str = "5";
  final int default_n_value = 5;
  int n = default_n_value;
  HashMap<String, TopNUniqueSort<E>> kmap = null;

  /**
   * Top N values per key are emitted
   *
   */
  public static final String KEY_N = "n";

  /**
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)
  {
    boolean ret = true;

    String nstr = config.get(KEY_N, default_n_str);
    try {
      Integer.parseInt(nstr);
    }
    catch (NumberFormatException e) {
      ret = false;
      throw new IllegalArgumentException(String.format("N has to be an integer(%s)", nstr));
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
    super.setup(config);
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }
    n = config.getInt(KEY_N, default_n_value);
    kmap = new HashMap<String, TopNUniqueSort<E>>();
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
    for (Map.Entry<String, E> e: ((HashMap<String, E>) payload).entrySet()) {
      TopNUniqueSort pqueue = kmap.get(e.getKey());
      if (pqueue == null) {
        pqueue = new TopNUniqueSort<E>(5, n, true);
        kmap.put(e.getKey(), pqueue);
      }
      pqueue.offer(e.getValue());
    }
  }

  @Override
  public void beginWindow()
  {
    kmap.clear();
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<String, TopNUniqueSort<E>> e: kmap.entrySet()) {
      HashMap<String, ArrayList> tuple = new HashMap<String, ArrayList>(1);
      tuple.put(e.getKey(), e.getValue().getTopN());
      emit(tuple);
    }
  }
}
