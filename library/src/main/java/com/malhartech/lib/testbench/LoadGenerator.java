/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractInputModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.Sink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Generates synthetic load. Creates tuples and keeps emitting them on the output port "data"<p>
 * <br>
 * The load is generated as per config parameters. This class is mainly meant for testing
 * nodes.<br>
 * It does not need to be windowed. It would just create tuple stream upto the limit set
 * by the config parameters.<br>
 * <br>
 * This node has been benchmarked at over 15 million tuples/second for String objects in local/inline mode<br>
 * <br>
 * <b>Tuple Schema</b>: Has two choices HashMap<String, Double>, or String<br><br>
 * <b>Benchmarks></b>:
 * String schema does about 4 Million tuples/sec in throughput<br>
 * HashMap schema does about 2.5 Million tuples/sec in throughput<br>
 * <b>Port Interface</b>:It has only one output port "data" and has no input ports<br><br>
 * <b>Properties</b>:
 * <b>keys</b> is a comma separated list of keys. This key are the <key> field in the tuple<br>
 * <b>values</b> are comma separated list of values. This value is the <value> field in the tuple. If not specified the values for all keys are 0.0<br>
 * <b>weights</b> are comma separated list of probability weights for each key. If not specified the weights are even for all keys<br>
 * <b>tuples_blast</b> is the total number of tuples sent out before the thread sleeps. The default value is 10000<br>
 * <b>sleep_time</b> is the sleep time for the thread between batch of tuples being sent out. The default value is 50<br>
 * <b>max_windows_count</b>The number of windows after which the node would shut down. If not set, the node runs forever<br>
 * <b>string_schema</b> controls the tuple schema. For string set it to "true". By default it is "false" (i.e. HashMap schema)<br>
 * <br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty<br>
 * <b>values</b> if specified has to be comma separated doubles and their number must match the number of keys<br>
 * <b>weights</b> if specified has to be comma separated integers and number of their number must match the number of keys<br>
 * <b>tuples_blast</b>If specified must be an integer<br>
 * <br>
 *
 * Compile time error checking includes<br>
 *
 *
 * @author amol
 */
@ModuleAnnotation(ports = {
  @PortAnnotation(name = LoadGenerator.OPORT_DATA, type = PortAnnotation.PortType.OUTPUT),
  @PortAnnotation(name = LoadGenerator.OPORT_COUNT, type = PortAnnotation.PortType.OUTPUT)
})
public class LoadGenerator extends AbstractInputModule
{
  public static final String OPORT_DATA = "data";
  public static final String OPORT_COUNT = "count";
  public static final String OPORT_COUNT_TUPLE_AVERAGE = "avg";
  public static final String OPORT_COUNT_TUPLE_COUNT = "count";
  private static Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);
  final int sleep_time_default_value = 50;
  final int tuples_blast_default_value = 10000;
  protected int tuples_blast = tuples_blast_default_value;
  protected int maxCountOfWindows = Integer.MAX_VALUE;
  protected int sleep_time = sleep_time_default_value;
  HashMap<String, Double> keys = new HashMap<String, Double>();
  HashMap<Integer, String> wtostr_index = new HashMap<Integer, String>();
  ArrayList<Integer> weights = null;
  boolean isstringschema = false;
  int total_weight = 0;
  private final Random random = new Random();
  private int rolling_window_count = 1;
  int[] tuple_numbers = null;
  int tuple_index = 0;
  int count_denominator = 1;
  private boolean count_connected = false;
  /**
   * keys are comma seperated list of keys for the load. These keys are send
   * one per tuple as per the other parameters
   *
   */
  public static final String KEY_KEYS = "keys";
  /**
   * values are to be assigned to each key. The tuple thus is a key,value
   * pair. The value field can either be empty (no value to any key), or a
   * comma separated list of values. If values list is provided, the number
   * must match the number of keys
   */
  public static final String KEY_VALUES = "values";
  /**
   * The weights define the probability of each key being assigned to current
   * tuple. The total of all weights is equal to 100%. If weights are not
   * specified then the probability is equal.
   */
  public static final String KEY_WEIGHTS = "weights";
  /**
   * The number of tuples sent out before the thread sleeps
   */
  public static final String KEY_TUPLES_BLAST = "tuples_blast";
  /**
   * The number of tuples sent out per milli second
   */
  public static final String KEY_SLEEP_TIME = "sleep_time";
  /**
   * The Maximum number of Windows to pump out.
   */
  public static final String MAX_WINDOWS_COUNT = "max_windows_count";
  /**
   * If specified as "true" a String class is sent, else HashMap is sent
   */
  public static final String KEY_STRING_SCHEMA = "string_schema";
  /**
   * The Maximum number of Windows to pump out.
   */
  public static final String ROLLING_WINDOW_COUNT = "rolling_window_count";

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
    String[] wstr = config.getTrimmedStrings(KEY_WEIGHTS);
    String[] kstr = config.getTrimmedStrings(KEY_KEYS);
    String[] vstr = config.getTrimmedStrings(KEY_VALUES);
    isstringschema = config.getBoolean(KEY_STRING_SCHEMA, false);
    String rstr = config.get(ROLLING_WINDOW_COUNT);


    boolean ret = true;

    if (kstr.length == 0) {
      ret = false;
      throw new IllegalArgumentException("Parameter \"key\" is empty");
    }
    else {
      LOG.debug(String.format("Number of keys are %d", kstr.length));
    }

    if (wstr.length == 0) {
      LOG.debug("weights was not provided, so keys would be equally weighted");
    }
    else {
      for (String s: wstr) {
        try {
          Integer.parseInt(s);
        }
        catch (NumberFormatException e) {
          ret = false;
          throw new IllegalArgumentException(String.format("Weight string should be an integer(%s)", s));
        }
      }
    }

    if (vstr.length == 0) {
      LOG.debug("values was not provided, so keys would have value of 0");
    }
    else {
      for (String s: vstr) {
        try {
          Double.parseDouble(s);
        }
        catch (NumberFormatException e) {
          ret = false;
          throw new IllegalArgumentException(String.format("Value string should be float(%s)", s));
        }
      }
    }

    if ((wstr.length != 0) && (wstr.length != kstr.length)) {
      ret = false;
      throw new IllegalArgumentException(
              String.format("Number of weights (%d) does not match number of keys (%d)",
                            wstr.length, kstr.length));
    }
    if ((vstr.length != 0) && (vstr.length != kstr.length)) {
      ret = false;
      throw new IllegalArgumentException(
              String.format("Number of values (%d) does not match number of keys (%d)",
                            vstr.length, kstr.length));
    }

    tuples_blast = config.getInt(KEY_TUPLES_BLAST, tuples_blast_default_value);
    if (tuples_blast <= 0) {
      ret = false;
      throw new IllegalArgumentException(
              String.format("tuples_blast (%d) has to be > 0", tuples_blast));
    }
    else {
      LOG.debug(String.format("tuples_blast set to %d", tuples_blast));
    }

    sleep_time = config.getInt(KEY_SLEEP_TIME, sleep_time_default_value);
    if (sleep_time <= 0) {
      ret = false;
      throw new IllegalArgumentException(
              String.format("sleep_time (%d) has to be > 0", sleep_time));
    }
    else {
      LOG.debug(String.format("sleep_time set to %d", sleep_time));
    }

    if (isstringschema) {
      if (vstr.length != 0) {
        LOG.debug(String.format("Value %s and stringschema is %s",
                                config.get(KEY_VALUES, ""), config.get(KEY_STRING_SCHEMA, "")));
        ret = false;
        throw new IllegalArgumentException(
                String.format("Value (\"%s\") cannot be specified if string_schema (\"%s\") is true",
                              config.get(KEY_VALUES, ""), config.get(KEY_STRING_SCHEMA, "")));
      }
    }

    if ((rstr != null) && !rstr.isEmpty()) {
      try {
        Integer.parseInt(rstr);
      }
      catch (NumberFormatException e) {
        ret = false;
        throw new IllegalArgumentException(String.format("%s has to be an integer (%s)", ROLLING_WINDOW_COUNT, rstr));
      }
    }

    maxCountOfWindows = config.getInt(MAX_WINDOWS_COUNT, Integer.MAX_VALUE);
    LOG.debug("{} set to generate data for {} windows", this, maxCountOfWindows);
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

    String[] wstr = config.getTrimmedStrings(KEY_WEIGHTS);
    String[] kstr = config.getTrimmedStrings(KEY_KEYS);
    String[] vstr = config.getTrimmedStrings(KEY_VALUES);

    isstringschema = config.getBoolean(KEY_STRING_SCHEMA, false);
    tuples_blast = config.getInt(KEY_TUPLES_BLAST, tuples_blast_default_value);
    sleep_time = config.getInt(KEY_SLEEP_TIME, sleep_time_default_value);
    rolling_window_count = config.getInt(ROLLING_WINDOW_COUNT, 1);

    if (rolling_window_count != 1) { // Initialized the tuple_numbers
      tuple_numbers = new int[rolling_window_count];
      for (int i = tuple_numbers.length; i > 0; i--) {
        tuple_numbers[i - 1] = 0;
      }
      tuple_index = 0;
    }

    // Keys and weights would are accessed via same key
    int i = 0;
    total_weight = 0;
    for (String s: kstr) {
      if (wstr.length != 0) {
        if (weights == null) {
          weights = new ArrayList<Integer>();
        }
        weights.add(Integer.parseInt(wstr[i]));
        total_weight += Integer.parseInt(wstr[i]);
      }
      else {
        total_weight += 1;
      }
      if (vstr.length != 0) {
        keys.put(s, new Double(Double.parseDouble(vstr[i])));
      }
      else {
        keys.put(s, 0.0);
      }
      wtostr_index.put(i, s);
      i += 1;
    }
  }

  /**
   * convenient method for not sending more than configured number of windows.
   */
  @Override
  public void endWindow()
  {
    //LOG.info(this +" endWindow: " + maxCountOfWindows + ", time=" + System.currentTimeMillis() + ", emitCount=" + emitCount);
    if (getProcessedTupleCount() > 0) {
      if (count_connected) {
        int tcount = getProcessedTupleCount();
        int average = 0;
        if (rolling_window_count == 1) {
          average = tcount;
        }
        else { // use tuple_numbers
          int denominator;
          if (count_denominator == rolling_window_count) {
            tuple_numbers[tuple_index] = tcount;
            denominator = rolling_window_count;
            tuple_index++;
            if (tuple_index == rolling_window_count) {
              tuple_index = 0;
            }
          }
          else {
            tuple_numbers[count_denominator - 1] = tcount;
            denominator = count_denominator;
            count_denominator++;
          }
          for (int i = 0; i < denominator; i++) {
            average += tuple_numbers[i];
          }
          average = average / denominator;
        }
        HashMap<String, Integer> tuples = new HashMap<String, Integer>();
        tuples.put(OPORT_COUNT_TUPLE_AVERAGE, new Integer(average));
        tuples.put(OPORT_COUNT_TUPLE_COUNT, new Integer(tcount));
        emit(OPORT_COUNT, tuples);
      }
      if (--maxCountOfWindows == 0) {
        deactivate();
      }
    }
  }

  @Override
  public void process(Object payload)
  {
    String tuple_key; // the tuple key
    int j = 0;

    for (int i = tuples_blast; i-- > 0;) {
      if (weights != null) { // weights are not even
        int rval = random.nextInt(total_weight);
        j = 0; // for randomization, need to reset to 0
        int wval = 0;
        for (Integer e: weights) {
          wval += e.intValue();
          if (wval >= rval) {
            break;
          }
          j++;
        }
      }
      else {
        j++;
        j = j % keys.size();
      }
      // j is the key index
      tuple_key = wtostr_index.get(j);
      if (!isstringschema) {
        HashMap<String, Double> tuple = new HashMap<String, Double>();
        tuple.put(tuple_key, keys.get(tuple_key));
        emit(OPORT_DATA, tuple);
      }
      else {
        emit(OPORT_DATA, tuple_key);
      }
    }
  }
}
