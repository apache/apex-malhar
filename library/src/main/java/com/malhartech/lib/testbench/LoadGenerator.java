/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractInputNode;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.NodeConfiguration;
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
 * <b>Port Interface</b>:It has only one output port "data" and has no input ports<br><br>
 * <b>Properties</b>:
 * <b>keys</b> is a comma separated list of keys. This key are the <key> field in the tuple<br>
 * <b>values</b> are comma separated list of values. This value is the <value> field in the tuple. If not specified the values for all keys are 0.0<br>
 * <b>weights</b> are comma separated list of probability weights for each key. If not specified the weights are even for all keys<br>
 * <b>tuples_per_sec</b> is the upper limit of number of tuples per sec. The default value is 10000. This library node has been benchmarked at over 15 million tuples/sec<br>
 * <b>string_schema</b> controls the tuple schema. For string set it to "true". By default it is "false" (i.e. HashMap schema)<br>
 * <br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty<br>
 * <b>values</b> if specified has to be comma separated doubles and their number must match the number of keys<br>
 * <b>weights</b> if specified has to be comma separated integers and number of their number must match the number of keys<br>
 * <b>tuples_per_sec</b>If specified must be an integer<br>
 * <br>
 *
 * Compile time error checking includes<br>
 *
 *
 * @author amol
 */
@NodeAnnotation(ports = {
  @PortAnnotation(name = LoadGenerator.OPORT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class LoadGenerator extends AbstractInputNode
{
  public static final String OPORT_DATA = "data";
  private static Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);
  protected volatile int tuples_per_sec = 10000;
  protected volatile int maxCountOfWindows = Integer.MAX_VALUE;
  HashMap<String, Double> keys = new HashMap<String, Double>();
  HashMap<Integer, String> wtostr_index = new HashMap<Integer, String>();
  ArrayList<Integer> weights = null;
  boolean isstringschema = false;
  int total_weight = 0;
  private final Random random = new Random();
  protected volatile boolean alive = true;
//  private final boolean outputConnected = false;
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
   * The number of tuples sent out per milli second
   */
  public static final String KEY_TUPLES_PER_SEC = "tuples_per_sec";
  /**
   * The Maximum number of Windows to pump out.
   */
  public static final String MAX_WINDOWS_COUNT = "max_windows_count";
  /**
   * If specified as "true" a String class is sent, else HashMap is sent
   */
  public static final String KEY_STRING_SCHEMA = "string_schema";

  /**
   *
   * Code to be moved to a proper base method name
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(NodeConfiguration config)
  {
    String[] wstr = config.getTrimmedStrings(KEY_WEIGHTS);
    String[] kstr = config.getTrimmedStrings(KEY_KEYS);
    String[] vstr = config.getTrimmedStrings(KEY_VALUES);
    isstringschema = config.getBoolean(KEY_STRING_SCHEMA, false);

    boolean ret = true;

    if (kstr.length == 0) {
      ret = false;
      throw new IllegalArgumentException("Parameter \"key\" is empty");
    }
    else {
      LOG.info(String.format("Number of keys are %d", kstr.length));
    }

    if (wstr.length == 0) {
      LOG.info("weights was not provided, so keys would be equally weighted");
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
      LOG.info("values was not provided, so keys would have value of 0");
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

    tuples_per_sec = config.getInt(KEY_TUPLES_PER_SEC, 10000);
    if (tuples_per_sec <= 0) {
      ret = false;
      throw new IllegalArgumentException(
              String.format("tuples_per_sec (%d) has to be > 0", tuples_per_sec));
    }
    else {
      LOG.info(String.format("tuples_per_sec set to %d", tuples_per_sec));
    }
    if (isstringschema) {
      if (vstr.length != 0) {
        LOG.info(String.format("Value %s and stringschema is %s",
                               config.get(KEY_VALUES, ""), config.get(KEY_STRING_SCHEMA, "")));
        ret = false;
        throw new IllegalArgumentException(
                String.format("Value (\"%s\") cannot be specified if string_schema (\"%s\") is true",
                              config.get(KEY_VALUES, ""), config.get(KEY_STRING_SCHEMA, "")));
      }
    }

    maxCountOfWindows = config.getInt(MAX_WINDOWS_COUNT, Integer.MAX_VALUE);
    LOG.debug("{} set to generate data for {} widows", this, maxCountOfWindows);
    return ret;
  }

  /**
   * Sets up all the config parameters. Assumes checking is done and has passed
   *
   * @param config
   */
  @Override
  public void setup(NodeConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }

    String[] wstr = config.getTrimmedStrings(KEY_WEIGHTS);
    String[] kstr = config.getTrimmedStrings(KEY_KEYS);
    String[] vstr = config.getTrimmedStrings(KEY_VALUES);

    isstringschema = config.getBoolean(KEY_STRING_SCHEMA, false);
    tuples_per_sec = config.getInt(KEY_TUPLES_PER_SEC, 10000);

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

//    /**
//     *
//     * To allow emit to wait till output port is connected in a deployment on Hadoop
//     * @param id
//     * @param dagpart
//     */
//    @Override
//    public void connected(String id, Sink dagpart) {
//        if (id.equals(OPORT_DATA)) {
//            outputConnected = true;
//        }
//    }
  // this is not the way to alive the load generation. there has to be a different way to handle that. Till then
  // keep an uppercap on how much you will generate.
//    /**
//     * The only way to shut down a loadGenerator. We are looking into a property based alive
//     */
//    public void deactivate() {
//        alive = true;
//        super.deactivate();
//    }

  @Override
  public void teardown() {
    this.alive = false; // TODO: need solution for alive in AbstractInputNode
    super.teardown();
  }

  /**
   * Generates all the tuples till alive (deactivate) is issued
   *
   * @param context
   */
  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void run()
  {
    int i = 0;

    while (alive) {
      HashMap<String, Double> tuple;
      String tuple_key; // the tuple key
      int j = 0;
      do {
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
          tuple = new HashMap<String, Double>();
          tuple.put(tuple_key, keys.get(tuple_key));
          emit(OPORT_DATA, tuple);
        }
        else {
          emit(OPORT_DATA, tuple_key);
        }
      } while (++i % tuples_per_sec != 0);

      try {
        Thread.sleep(5); // Remove sleep if you want to blast data at huge rate
      }
      catch (InterruptedException e) {
        LOG.error("Unexpected error while sleeping for 1 s", e);
        alive = false;
      }
    }
    LOG.info("Finished generating tuples");
  }

  /**
   * convenient method for not sending more than 10 windows.
   */
  @Override
  public void endWindow()
  {
    if (--maxCountOfWindows == 0) {
      alive = false;
    }
  }
}
