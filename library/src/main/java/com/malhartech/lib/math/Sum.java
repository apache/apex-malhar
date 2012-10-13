/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.Sink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". At end of window sums all values
 * for each key and emits them on port <b>sum</b>; emits number of occurrences on port <b>count</b>; and average on port <b>average</b><p>
 * <br> Values are stored in a
 * hash<br> This node only functions in a windowed stram application<br> Compile
 * time error processing is done on configuration parameters<br> input port
 * "data" must be connected<br> output port "sum" must be connected<br>
 * "windowed" has to be true<br> Run time error processing are emitted on _error
 * port. The errors are:<br> Value is not a Number<br>
 *
 * @author amol
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = Sum.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = Sum.OPORT_SUM, type = PortAnnotation.PortType.OUTPUT),
  @PortAnnotation(name = Sum.OPORT_AVERAGE, type = PortAnnotation.PortType.OUTPUT),
  @PortAnnotation(name = Sum.OPORT_COUNT, type = PortAnnotation.PortType.OUTPUT)
})
public class Sum extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_SUM = "sum";
  public static final String OPORT_AVERAGE = "average";
  public static final String OPORT_COUNT = "count";

  private static Logger LOG = LoggerFactory.getLogger(Sum.class);
  HashMap<String, Object> sum = new HashMap<String, Object>();
  boolean count_connected = false;
  boolean sum_connected = false;
  boolean average_connected = false;

  int num_unique_keys_default_value = 1000;
  int num_unique_keys = num_unique_keys_default_value;
  int newkey_location = 0;
  double [] sums = null;
  int [] counts = null;

  @Override
  public void connected(String id, Sink dagpart)
  {
    if (id.equals(OPORT_COUNT)) {
      count_connected = (dagpart != null);
    }
    else if (id.equals(OPORT_SUM)) {
      sum_connected = (dagpart != null);
    }
    else if (id.equals(OPORT_AVERAGE)) {
      average_connected = (dagpart != null);
    }
  }

  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    // Change this to do the following
    // Reserve 10,000 "int" array vals[...]
    // Use HashMap for the location of the key
    // Then on simply do vals[map.get(key).IntValue()]++;
    //

    for (Map.Entry<String, Number> e: ((HashMap<String, Number>) payload).entrySet()) {
      Integer sloc = (Integer) sum.get(e.getKey());
      int iloc = 0;
      if (sloc == null) {
        iloc = newkey_location;
        newkey_location++;
        if (newkey_location > num_unique_keys) {
          int newnum = 2 * num_unique_keys;
          double [] newsums = new double[newnum];
          int [] newcounts = new int[newnum];
              // System.arraycopy(sum, spinMillis, this, spinMillis, spinMillis);
          for (int i = 0; i < num_unique_keys; i++) {
            newsums[i] = sums[i];
            newcounts[i] = counts[i];
          }
          for (int i = num_unique_keys; i < newnum; i++) {
            newsums[i] = 0.0;
            newcounts[i] = 0;
          }
        }
        sum.put(e.getKey(), new Integer(iloc));
      }
      else {
        iloc = sloc.intValue();
      }
      sums[iloc] += e.getValue().doubleValue();
      counts[iloc]++;
    }
  }

  public boolean myValidation(ModuleConfiguration config)
  {
    return true;
  }


  /**
   * Sets up all the config parameters. Assumes checking is done and has passed
   *
   * @param config
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new IllegalArgumentException("Did not pass validation");
    }

    // Initialize sums and counts to 0
    num_unique_keys = num_unique_keys_default_value;
    sums = new double[num_unique_keys];
    counts = new int[num_unique_keys];
    newkey_location = 0;
    // System.arraycopy(sum, spinMillis, this, spinMillis, spinMillis);
    for (int i = 0; i < num_unique_keys; i++) {
      sums[i] = 0.0;
      counts[i] = 0;
    }
  }

  @Override
  public void beginWindow()
  {
    sum.clear();
    newkey_location = 0;
  }

  /**
   * Node only works in windowed mode. Emits all data upon end of window tuple
   */
  @Override
  public void endWindow()
  {

    // Should allow users to send each key as a separate tuple to load balance
    // This is an aggregate node, so load balancing would most likely not be needed

    HashMap<String, Object> stuples = null;
    if (sum_connected) {
      stuples = new HashMap<String, Object>();
    }

    HashMap<String, Object> ctuples = null;
    if (count_connected) {
      ctuples = new HashMap<String, Object>();
    }

    HashMap<String, Object> atuples = null;
    if (average_connected) {
      atuples = new HashMap<String, Object>();
    }

    for (Map.Entry<String, Object> e: sum.entrySet()) {
      int location = ((Integer) e.getValue()).intValue();
      if (sum_connected) {
        stuples.put(e.getKey(), new Double(sums[location]));
        sums[location] = 0.0;
      }
      if (count_connected) {
        ctuples.put(e.getKey(), new Integer(counts[location]));
        counts[location] = 0;
      }
      if (average_connected) {
        if (counts[location] != 0) { // should always be true
          atuples.put(e.getKey(), new Double(sums[location]/counts[location]));
        }
      }
    }

    if ((stuples != null) && !stuples.isEmpty()) {
      emit(OPORT_SUM, stuples);
    }
    if ((ctuples != null) && !ctuples.isEmpty()) {
      emit(OPORT_COUNT, ctuples);
    }
    if ((atuples != null) && !atuples.isEmpty()) {
      emit(OPORT_AVERAGE, atuples);
    }
  }
}
