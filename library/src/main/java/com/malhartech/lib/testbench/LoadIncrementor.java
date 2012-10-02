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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
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
 * Benchmarks: This node has been benchmarked at over ?? million tuples/second in local/inline mode<br>
 *
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
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = LoadIncrementor.IPORT_SEED, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = LoadIncrementor.IPORT_INCREMENT, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = LoadIncrementor.OPORT_DATA, type = PortAnnotation.PortType.OUTPUT),
  @PortAnnotation(name = LoadIncrementor.OPORT_COUNT, type = PortAnnotation.PortType.OUTPUT)
})
public class LoadIncrementor extends AbstractModule
{
  public static final String IPORT_SEED = "seed";
  public static final String IPORT_INCREMENT = "increment";
  public static final String OPORT_DATA = "data";
  public static final String OPORT_COUNT = "count";

  private static Logger LOG = LoggerFactory.getLogger(LoadIncrementor.class);

  HashMap<String, Object> vmap = new HashMap<String, Object>();
  String[] keys = null;
  double[] low_limits = null;
  double[] high_limits = null;

  float delta_default_value = 1;
  float delta = delta_default_value;

  int tuple_count = 0;

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
    String[] lkey = config.get(KEY_LIMITS).split(";");

    low_limits = new double[lkey.length];
    high_limits = new double[lkey.length];
    keys = new String[kstr.length];
    int j = 0;
    for (String l : lkey) {
      String[] limits = l.split(",");
      keys[j] = kstr[j];
      low_limits[j] = Double.valueOf(limits[0]).doubleValue();
      high_limits[j] = Double.valueOf(limits[1]).doubleValue();
      j++;
    }
  }

  public double getNextNumber(double current, double increment, double low, double high ) {
    return current;

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
        if (keys.length != ((ArrayList) e.getValue()).size()) { // bad seed
          // emit error tuple here
        }
        else {
          ArrayList alist = new ArrayList();
          int j = 0;
          for (Integer s: (ArrayList<Integer>) e.getValue()) {
            valueData d = new valueData(keys[j], new Double(s.doubleValue()));
            alist.add(d);
            j++;
          }
          vmap.put(e.getKey(), alist);
        }
      }
    }
    else if (IPORT_INCREMENT.equals(getActivePort())) {
      for (Map.Entry<String, Object> e: ((HashMap<String, Object>) payload).entrySet()) {
        String key = e.getKey(); // the key
        ArrayList<valueData> alist = (ArrayList<valueData>) vmap.get(key); // does it have a location?
        if (alist != null) { // if not seeded just ignore
          for (Map.Entry<String, Integer> o : ((HashMap<String, Integer>) e.getValue()).entrySet()) {
            String dimension = o.getKey();
            int j = 0;
            int cur_slot = 0;
            int new_slot = 0;
            for (valueData d : alist) {
              if (dimension.equals(d.str)) {
                // Compute the new location
                Double current = (Double) d.value;
                cur_slot = current.intValue();
                alist.get(j).value = getNextNumber(current.doubleValue(), (delta/100) * (o.getValue().intValue() % 100), low_limits[j], high_limits[j]);
                new_slot = ((Double) alist.get(j).value).intValue();
                break;
              }
              j++;
            }
            if (cur_slot != new_slot) {
              HashMap<String, String> tuple = new HashMap<String, String>();
              String val = new String();
              for (valueData d : alist) {
                if (!val.isEmpty()) {
                  val += ",";
                }
                Integer ival = ((Double) d.value).intValue();
                val += ival.toString();
              }
              tuple.put(key, val);
              emit(OPORT_DATA, tuple);
            }
          }
        }
        else { // oops, no seed yet
          ;
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
    HashMap<String, Integer> tuple = new HashMap<String, Integer>();
    tuple.put("TUPLE_COUNT", new Integer(tuple_count));
    emit(OPORT_COUNT, tuple);
  }

}
