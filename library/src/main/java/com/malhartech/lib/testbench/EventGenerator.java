/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.api.AsyncInputOperator;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.dag.AsyncInputNode;
import com.malhartech.api.FailedOperationException;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.OperatorContext;
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
 * <b>Tuple Schema</b>: Has two choices HashMap<String, Double>, or String<br><br>
 * <b>Benchmarks></b>: Send as many tuples in in-line mode, the receiver just counts the tuples and drops the object<br>
 * String schema does about 26 Million tuples/sec in throughput<br>
 * HashMap schema does about 10 Million tuples/sec in throughput<br>
 * <b>Port Interface</b>:It has only one output port "data" and has no input ports<br><br>
 * <b>Properties</b>:
 * <b>keys</b> is a comma separated list of keys. This key are the <key> field in the tuple<br>
 * <b>values</b> are comma separated list of values. This value is the <value> field in the tuple. If not specified the values for all keys are 0.0<br>
 * <b>weights</b> are comma separated list of probability weights for each key. If not specified the weights are even for all keys<br>
 * <b>tuples_blast</b> is the total number of tuples sent out before the thread returns control. The default value is 10000<br>
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
public class LoadGenerator implements AsyncInputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);
  public final transient DefaultOutputPort<String> string_data = new DefaultOutputPort<String>(this);
  public final transient DefaultOutputPort<HashMap<String, Double>> hash_data = new DefaultOutputPort<HashMap<String, Double>>(this);
  public final transient DefaultOutputPort<HashMap<String, Number>> count = new DefaultOutputPort<HashMap<String, Number>>(this);
  public static final String OPORT_COUNT_TUPLE_AVERAGE = "avg";
  public static final String OPORT_COUNT_TUPLE_COUNT = "count";
  public static final String OPORT_COUNT_TUPLE_TIME = "window_time";
  public static final String OPORT_COUNT_TUPLE_TUPLES_PERSEC = "tuples_per_sec";
  public static final String OPORT_COUNT_TUPLE_WINDOWID = "window_id";
  protected static final int TUPLES_BLAST_DEFAULT = 10000;
  private transient int tuples_blast = TUPLES_BLAST_DEFAULT;
  protected transient int maxCountOfWindows = Integer.MAX_VALUE;
  transient HashMap<String, Double> keys = new HashMap<String, Double>();
  transient HashMap<Integer, String> wtostr_index = new HashMap<Integer, String>();
  transient ArrayList<Integer> weights;
  transient int total_weight = 0;
  private transient final Random random = new Random();
  public static final int ROLLING_WINDOW_COUNT_DEFAULT = 1;
  private transient int rolling_window_count = ROLLING_WINDOW_COUNT_DEFAULT;
  transient long[] tuple_numbers = null;
  transient long[] time_numbers = null;
  transient int tuple_index = 0;
  transient int count_denominator = 1;
  transient int count_windowid = 0;
  private transient long windowStartTime = 0;
  /**
   * keys are comma separated list of keys for the load. These keys are send
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
   * The number of tuples sent out before it returns control. Users need to ensure that this does not cross window boundary
   */
  public static final String KEY_TUPLES_BLAST = "tuples_blast";
  /**
   * The Maximum number of Windows to pump out.
   */
  public static final String MAX_WINDOWS_COUNT = "max_windows_count";
  /**
   * If specified as "true" a String class is sent, else HashMap is sent
   */
  public static final String KEY_STRING_SCHEMA = "string_schema";
  private int generatedTupleCount;
  private String[] key_keys = new String[0];
  private String[] key_weights = new String[0];
  private String[] key_values = new String[0];

  /**
   *
   * Code to be moved to a proper base method name
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(OperatorConfiguration config)
  {
    boolean ret = true;

    if (key_keys.length == 0) {
      ret = false;
      throw new IllegalArgumentException("Parameter \"key\" is empty");
    }
    else {
      LOG.debug(String.format("Number of keys are %d", key_keys.length));
    }

    if (key_weights.length == 0) {
      LOG.debug("weights was not provided, so keys would be equally weighted");
    }
    else {
      for (String s: key_weights) {
        try {
          Integer.parseInt(s);
        }
        catch (NumberFormatException e) {
          ret = false;
          throw new IllegalArgumentException(String.format("Weight string should be an integer(%s)", s));
        }
      }
    }

    if (key_values.length == 0) {
      LOG.debug("values was not provided, so keys would have value of 0");
    }
    else {
      for (String s: key_values) {
        try {
          Double.parseDouble(s);
        }
        catch (NumberFormatException e) {
          ret = false;
          throw new IllegalArgumentException(String.format("Value string should be float(%s)", s));
        }
      }
    }

    if ((key_weights.length != 0) && (key_weights.length != key_keys.length)) {
      ret = false;
      throw new IllegalArgumentException(
              String.format("Number of weights (%d) does not match number of keys (%d)",
                            key_weights.length, key_keys.length));
    }
    if ((key_values.length != 0) && (key_values.length != key_keys.length)) {
      ret = false;
      throw new IllegalArgumentException(
              String.format("Number of values (%d) does not match number of keys (%d)",
                            key_values.length, key_keys.length));
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
  public void setup(OperatorConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }

    maxCountOfWindows = config.getInt(MAX_WINDOWS_COUNT, Integer.MAX_VALUE);

    if (rolling_window_count != 1) { // Initialized the tuple_numbers
      tuple_numbers = new long[rolling_window_count];
      time_numbers = new long[rolling_window_count];
      for (int i = tuple_numbers.length; i > 0; i--) {
        tuple_numbers[i - 1] = 0;
        time_numbers[i - 1] = 0;
      }
      tuple_index = 0;
    }

    // Keys and weights would are accessed via same key
    int i = 0;
    total_weight = 0;
    for (String s: key_keys) {
      if (key_weights.length != 0) {
        if (weights == null) {
          weights = new ArrayList<Integer>();
        }
        weights.add(Integer.parseInt(key_weights[i]));
        total_weight += Integer.parseInt(key_weights[i]);
      }
      else {
        total_weight += 1;
      }
      if (key_values.length != 0) {
        keys.put(s, new Double(Double.parseDouble(key_values[i])));
      }
      else {
        keys.put(s, 0.0);
      }
      wtostr_index.put(i, s);
      i += 1;
    }
  }

  @Override
  public void beginWindow()
  {
    if (count.isConnected()) {
      generatedTupleCount = 0;
      windowStartTime = System.currentTimeMillis();
    }
  }

  /**
   * convenient method for not sending more than configured number of windows.
   */
  @Override
  public void endWindow()
  {
    //LOG.info(this +" endWindow: " + maxCountOfWindows + ", time=" + System.currentTimeMillis() + ", emitCount=" + emitCount);
    if (count.isConnected() && generatedTupleCount > 0) {
      long elapsedTime = System.currentTimeMillis() - windowStartTime;
      if (elapsedTime == 0) {
        elapsedTime = 1; // prevent from / zero
      }

      int tcount = generatedTupleCount;
      long average;
      if (rolling_window_count == 1) {
        average = (tcount * 1000) / elapsedTime;
      }
      else { // use tuple_numbers
        int slots;
        if (count_denominator == rolling_window_count) {
          tuple_numbers[tuple_index] = tcount;
          time_numbers[tuple_index] = elapsedTime;
          slots = rolling_window_count;
          tuple_index++;
          if (tuple_index == rolling_window_count) {
            tuple_index = 0;
          }
        }
        else {
          tuple_numbers[count_denominator - 1] = tcount;
          time_numbers[count_denominator - 1] = elapsedTime;
          slots = count_denominator;
          count_denominator++;
        }
        long time_slot = 0;
        long num_tuples = 0;
        for (int i = 0; i < slots; i++) {
          num_tuples += tuple_numbers[i];
          time_slot += time_numbers[i];
        }
        average = (num_tuples * 1000) / time_slot; // as the time is in millis
      }
      HashMap<String, Number> tuples = new HashMap<String, Number>();
      tuples.put(OPORT_COUNT_TUPLE_AVERAGE, new Long(average));
      tuples.put(OPORT_COUNT_TUPLE_COUNT, new Integer(tcount));
      tuples.put(OPORT_COUNT_TUPLE_TIME, new Long(elapsedTime));
      tuples.put(OPORT_COUNT_TUPLE_TUPLES_PERSEC, new Long((tcount * 1000) / elapsedTime));
      tuples.put(OPORT_COUNT_TUPLE_WINDOWID, new Integer(count_windowid++));
      count.emit(tuples);
    }

    if (--maxCountOfWindows == 0) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void injectTuples(long windowId)
  {
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
      String tuple_key = wtostr_index.get(j);

      if (string_data.isConnected()) {
        string_data.emit(tuple_key);
      }

      if (hash_data.isConnected()) {
        HashMap<String, Double> tuple = new HashMap<String, Double>(1);
        tuple.put(tuple_key, keys.get(tuple_key));
        hash_data.emit(tuple);
      }
    }
  }

  @Override
  public void activated(OperatorContext context)
  {
  }

  @Override
  public void deactivated()
  {
  }

  @Override
  public void teardown()
  {
  }

  public void setKeys(String key)
  {
    key_keys = key.split(",");
  }

  public void setWeights(String weight)
  {
    key_weights = weight.split(",");
  }

  public void setValues(String value)
  {
    key_values = value.split(",");
  }

  /**
   * @param tuples_blast the tuples_blast to set
   */
  public void setTuplesBlast(int tuples_blast)
  {
    this.tuples_blast = tuples_blast;
  }

  /**
   * @param rolling_window_count the rolling_window_count to set
   */
  public void setRollingWindowCount(int rolling_window_count)
  {
    this.rolling_window_count = rolling_window_count;
  }
}
