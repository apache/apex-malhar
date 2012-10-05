/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

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
 * Takes in a seed stream on port <b>seed</b> and then on increments this data based on increments on port <b>increment</b>.
 * Data is immediately emitted on output port <b>data</b>.<p>
 * The aim is to create a random movement
 * <br>
 * Examples of application includes<br>
 * random motion<br>
 * <br>
 * <br>
 * Description: tbd
 * <br>
 * Benchmarks: The benchmark was done in local/inline mode<br>
 * Processing tuples on seed port are at 3.5 Million tuples/sec<br>
 * Processing tuples on increment port are at 10 Million tuples/sec<br>
 * <br>
 * <b>Tuple Schema</b>: Each tuple is HashMap<String, ArrayList> on both the ports. Currently other schemas are not supported<br>
 * <b>Port Interface</b><br>
 * <b>seed</b>: The seed data for setting up the incrementor data to work on<br>
 * <b>increment</b>: Small random increments to the seed data. This now creates a randomized change in the seed<br>
 * <b>data</b>: Output of seed + increment<br>
 * <br>
 * <b>Properties</b>:
 * <br>keys: In case the value has multiple dimensions. They can be accessed via keys<br>
 * <br>delta: The max value from an increment. The value on increment port is treated as a "percent" of this delta<br>
 * Compile time checks are:<br>
 * <br>
 *
 * @author amol
 */
@ModuleAnnotation(ports = {
  @PortAnnotation(name = LoadIncrementer.IPORT_SEED, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = LoadIncrementer.IPORT_INCREMENT, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = LoadIncrementer.OPORT_DATA, type = PortAnnotation.PortType.OUTPUT),
  @PortAnnotation(name = LoadIncrementer.OPORT_COUNT, type = PortAnnotation.PortType.OUTPUT)
})
public class LoadIncrementer extends AbstractModule
{
  public static final String IPORT_SEED = "seed";
  public static final String IPORT_INCREMENT = "increment";
  public static final String OPORT_DATA = "data";
  public static final String OPORT_COUNT = "count";
  public static final String OPORT_COUNT_TUPLE_COUNT = "count";
  private static Logger LOG = LoggerFactory.getLogger(LoadIncrementer.class);
  HashMap<String, Object> vmap = new HashMap<String, Object>();
  String[] keys = null;
  double[] low_limits = null;
  double[] high_limits = null;
  double sign = -1.0;
  final double low_limit_default_val = 0;
  final double high_limit_default_val = 100;
  float delta_default_value = 1;
  float delta = delta_default_value;
  int tuple_count = 0;
  private transient boolean count_connected = false;
  /**
   * keys are comma separated list of keys for seeding. They are taken in order on seed port (i.e. keys need not be sent)<p>
   * On the increment port changes are sent per key.<br>
   * If not provided the keys are ignored (i.e. behaves like single value)<br>
   *
   */
  public static final String KEY_KEYS = "keys";
  /**
   * delta defines what constitutes a change. Default value is 1<p>
   * <br>
   */
  public static final String KEY_DELTA = "delta";
  public static final String KEY_LIMITS = "limits";

  // Data Recieved on seed port
  class valueData
  {
    String str;
    Object value;

    valueData(String istr, Object val)
    {
      str = istr;
      value = val;
    }
  }

  /**
   *
   * @param id
   * @param dagpart
   */
  @Override
  public void connected(String id, Sink dagpart)
  {
    if (id.equals(OPORT_COUNT)) {
      count_connected = (dagpart != null);
    }
  }

  /**
   *
   * Code to be moved to a proper base method name
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)
  {
    boolean ret = true;
    //delta = config.getFloat(KEY_DELTA, delta_default_value);
    String key_str = config.get(KEY_KEYS);
    String limits_str = config.get(KEY_LIMITS);

    if (key_str.isEmpty()) {
      ret = false;
      throw new IllegalArgumentException("Property \"keys\" is empty");
    }

    if (!limits_str.isEmpty()) { // if specified, must match the key_str ids
      String[] skey = key_str.split(",");
      String[] lkey = limits_str.split(";");
      if (skey.length != lkey.length) {
        ret = false;
        throw new IllegalArgumentException(
                String.format("Number of ids in keys(%d) does not match number ids in limits (%d)", skey.length, lkey.length));
      }
      // ensure lkey has two doubles each
      for (String l: lkey) {
        String[] klimit = l.split(",");
        if (klimit.length != 2) {
          ret = false;
          throw new IllegalArgumentException(
                  String.format("Property \"limits\" has a illegal value (%s). Need to be \"lower_limit,upper_limit\"", l));
        }
        for (String k: klimit) {
          try {
            Double.parseDouble(k);
          }
          catch (NumberFormatException e) {
            ret = false;
            throw new IllegalArgumentException(String.format("Property \"limits\" has illegal format for one of its strings (%s)", k));
          }
        }
        Double low_limit = Double.parseDouble(klimit[0]);
        Double high_limit = Double.parseDouble(klimit[1]);
        if (low_limit >= high_limit) {
          ret = false;
          throw new IllegalArgumentException(String.format("Property \"limits\" low value (%s) >= high_value(%s)", klimit[0], klimit[1]));
        }
      }
    }
    return ret;
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
      throw new FailedOperationException("Did not pass validation");
    }

    delta = config.getFloat(KEY_DELTA, delta_default_value);
    String[] kstr = config.getTrimmedStrings(KEY_KEYS);

    low_limits = new double[kstr.length];
    high_limits = new double[kstr.length];
    keys = new String[kstr.length];
    int j = 0;
    for (String k: kstr) {
      keys[j] = k;
      low_limits[j] = low_limit_default_val;
      high_limits[j] = high_limit_default_val;
      j++;
    }

    String lkey = config.get(KEY_LIMITS);
    if (!lkey.isEmpty()) {
      String[] lstr = lkey.split(";");
      j = 0;
      for (String l: lstr) {
        String[] klimit = l.split(",");
        low_limits[j] = Double.valueOf(klimit[0]).doubleValue();
        high_limits[j] = Double.valueOf(klimit[1]).doubleValue();
        j++;
      }
    }

    for (int i = 0; i < kstr.length; i++) {
      LOG.debug(String.format("Key %s has limits %f,%f", keys[i], low_limits[i], high_limits[i]));
    }
  }

  public double getNextNumber(double current, double increment, double low, double high)
  {
    double ret = current;
    double range = high - low;
    if (increment > range) { // bad data, do nothing
      ret = current;
    }
    else {
      sign = sign * -1.0;
      ret += sign * increment;
      if (ret < low) {
        ret = (low + high) / 2;
      }
      if (ret > high) {
        ret = (low + high) / 2;
      }
    }
    return ret;
  }

  /**
   *
   * @param key tuple key in HashMap<key, value>
   * @param list list of data items
   */
  public void emitDataTuple(String key, ArrayList list)
  {
    HashMap<String, String> tuple = new HashMap<String, String>(1);
    String val = new String();
    for (valueData d: (ArrayList<valueData>)list) {
      if (!val.isEmpty()) {
        val += ",";
      }
      Integer ival = ((Double)d.value).intValue();
      val += ival.toString();
    }
    tuple.put(key, val);
    emit(OPORT_DATA, tuple);
  }

  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    // LoadSeedGenerator would provide seed
    // LoadRandomGenerator->SeedClassifier would provide Increment, use delta to make it fit

    tuple_count++;
    if (IPORT_SEED.equals(getActivePort())) {
      // Payload is     HashMap<String, Object> ret = new HashMap<String, Object>();, where Object is ArrayList of Integers
      // Allow Seed to override
      for (Map.Entry<String, ArrayList> e: ((HashMap<String, ArrayList>)payload).entrySet()) {
        if (keys.length != ((ArrayList)e.getValue()).size()) { // bad seed
          // emit error tuple here
        }
        else {
          ArrayList<Integer> ilist = (ArrayList<Integer>)e.getValue();
          ArrayList alist = new ArrayList(ilist.size());
          int j = 0;
          for (Integer s: ilist) {
            valueData d = new valueData(keys[j], new Double(s.doubleValue()));
            alist.add(d);
            j++;
          }
          vmap.put(e.getKey(), alist);
          emitDataTuple(e.getKey(), alist);
        }
      }
    }
    else if (IPORT_INCREMENT.equals(getActivePort())) {
      for (Map.Entry<String, Object> e: ((HashMap<String, Object>)payload).entrySet()) {
        String key = e.getKey(); // the key
        ArrayList<valueData> alist = (ArrayList<valueData>)vmap.get(key); // does it have a location?
        if (alist != null) { // if not seeded just ignore
          for (Map.Entry<String, Integer> o: ((HashMap<String, Integer>)e.getValue()).entrySet()) {
            String dimension = o.getKey();
            int j = 0;
            int cur_slot = 0;
            int new_slot = 0;
            for (valueData d: alist) {
              if (dimension.equals(d.str)) {
                // Compute the new location
                cur_slot = ((Double)d.value).intValue();
                Double nval = getNextNumber(((Double)d.value).doubleValue(), delta / 100 * (o.getValue().intValue() % 100), low_limits[j], high_limits[j]);
                new_slot = nval.intValue();
                alist.get(j).value = nval;
                break;
              }
              j++;
            }
            if (cur_slot != new_slot) {
              emitDataTuple(key, alist);
            }
          }
        }
        else { // oops, no seed yet
        }
      }
    }
  }

  @Override
  public void beginWindow()
  {
    tuple_count = 0;
  }

  @Override
  public void endWindow()
  {
    if (count_connected) {
      HashMap<String, Integer> tuple = new HashMap<String, Integer>(1);
      tuple.put(OPORT_COUNT_TUPLE_COUNT, new Integer(tuple_count));
      emit(OPORT_COUNT, tuple);
    }
  }
}
