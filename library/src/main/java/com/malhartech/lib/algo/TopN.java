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
 * Takes a stream of key value pairs via input port "data", and they are ordered by key. Top N of the ordered tuples per key are emitted on port "top" at the end of window<p>
 * This is an end of window module<br>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects HashMap<String, Object><br>
 * <b>top</b>: Output data port, emits HashMap<String, Object><br>
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
public class TopN extends OrderByKey
{
  private static Logger LOG = LoggerFactory.getLogger(TopN.class);

  final String default_n_str = new String("5");
  final int default_n_value = 5;
  int n = default_n_value;


  /**
   * Top N values per key are emitted
   *
   */
  public static final String KEY_N = "n";

  /**
   *
   * Takes in a key and an arrayIndex. ReverseIndexes the strings in the ArrayIndex
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    super.process(payload);
    int size = pqueue.size();
    if (size > n) {
      // purge the extra value here from pqueue and smap
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
  }
}
