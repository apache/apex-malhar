/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.testbench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 *
 * Generates synthetic load.&nbsp; Creates tuples and keeps emitting them on the output port "data".
 * <p>
 * <br>
 * The load is generated as per config parameters. This class is mainly meant for testing
 * nodes.<br>
 * It does not need to be windowed. It would just create tuple stream upto the limit set
 * by the config parameters.<br>
 * <b>Ports</b>:
 * <b>string_data</b>: emits String<br>
 * <b>hash_data</b>: emits HashMap<String,Double><br>
 * <b>count<b>: emits HashMap<String, Number>, contains per window count of throughput<br>
 * <br>
 * <b>Tuple Schema</b>: Has two choices HashMap<String, Double>, or String<br><br>
 * <b>Port Interface</b>:It has only one output port "data" and has no input ports<br><br>
 * <b>Properties</b>:
 * <b>keys</b> is a comma separated list of keys. This key are the <key> field in the tuple<br>
 * <b>values</b> are comma separated list of values. This value is the <value> field in the tuple. If not specified the values for all keys are 0.0<br>
 * <b>weights</b> are comma separated list of probability weights for each key. If not specified the weights are even for all keys<br>
 * <b>tuples_blast</b> is the total number of tuples sent out before the thread returns control. The default value is 10000<br>
 * <b>max_windows_count</b>The number of windows after which the node would shut down. If not set, the node runs forever<br>
 * <br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty<br>
 * <b>values</b> if specified has to be comma separated doubles and their number must match the number of keys<br>
 * <b>weights</b> if specified has to be comma separated integers and number of their number must match the number of keys<br>
 * <b>tuples_blast</b>If specified must be an integer<br>
 * <br>
 *
 * Compile time error checking includes<br>
 * <b>Benchmarks></b>: Send as many tuples in in-line mode, the receiver just counts the tuples and drops the object<br>
 * String schema does about 26 Million tuples/sec in throughput<br>
 * HashMap schema does about 10 Million tuples/sec in throughput<br>
 * @displayName Event Generator
 * @category Testbench
 * @tags input operator, generator
 * @since 0.3.2
 */
public class EventGenerator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(EventGenerator.class);
  
  /**
   * Output string port that emits string data.
   */
  public final transient DefaultOutputPort<String> string_data = new DefaultOutputPort<String>();
  
  /**
   * Output hash data port that emits a hashmap of &lt;string,double&gt;.
   */
  public final transient DefaultOutputPort<HashMap<String, Double>> hash_data = new DefaultOutputPort<HashMap<String, Double>>();
  
  /**
   * Output count port that emits a hashmap of &lt;string,number&gt; which contains per window count of throughput.
   */
  public final transient DefaultOutputPort<HashMap<String, Number>> count = new DefaultOutputPort<HashMap<String, Number>>();
  public static final String OPORT_COUNT_TUPLE_AVERAGE = "avg";
  public static final String OPORT_COUNT_TUPLE_COUNT = "count";
  public static final String OPORT_COUNT_TUPLE_TIME = "window_time";
  public static final String OPORT_COUNT_TUPLE_TUPLES_PERSEC = "tuples_per_sec";
  public static final String OPORT_COUNT_TUPLE_WINDOWID = "window_id";
  protected static final int TUPLES_BLAST_DEFAULT = 10000;

  @Min(1)
  private int tuples_blast = TUPLES_BLAST_DEFAULT;

  @Min(1)
  protected int maxCountOfWindows = Integer.MAX_VALUE;
  HashMap<String, Double> keys = new HashMap<String, Double>();
  HashMap<Integer, String> wtostr_index = new HashMap<Integer, String>();
  ArrayList<Integer> weights;
  int total_weight = 0;
  private transient final Random random = new Random();
  public static final int ROLLING_WINDOW_COUNT_DEFAULT = 1;
  @Min(1)
  private int rolling_window_count = ROLLING_WINDOW_COUNT_DEFAULT;
  transient long[] tuple_numbers = null;
  transient long[] time_numbers = null;
  transient int tuple_index = 0;
  transient int count_denominator = 1;
  transient int count_windowid = 0;
  private transient long windowStartTime = 0;
  private transient int generatedTupleCount = 0;
  @NotNull
  private String keysHelper = null;
  private String weightsHelper = null;

  private String valuesHelper = null;

  @NotNull
  private String[] keysArray = null;

  private String[] weightsArray = null;
  private String[] valuesArray = null;

  /**
   *
   * Sets up all the config parameters. Assumes checking is done and has passed
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    if (rolling_window_count != 1) { // Initialized the tuple_numbers
      tuple_numbers = new long[rolling_window_count];
      time_numbers = new long[rolling_window_count];
      for (int i = tuple_numbers.length; i > 0; i--) {
        tuple_numbers[i - 1] = 0;
        time_numbers[i - 1] = 0;
      }
      tuple_index = 0;
    }

    // Keys and weights are accessed via same key
    int i = 0;
    total_weight = 0;
    for (String s: keysArray) {
      if ((weightsArray != null) && weightsArray.length != 0) {
        if (weights == null) {
          weights = new ArrayList<Integer>();
        }
        weights.add(Integer.parseInt(weightsArray[i]));
        total_weight += Integer.parseInt(weightsArray[i]);
      }
      else {
        total_weight += 1;
      }
      if ((valuesArray != null) && valuesArray.length != 0) {
        keys.put(s, new Double(Double.parseDouble(valuesArray[i])));
      }
      else {
        keys.put(s, 0.0);
      }
      wtostr_index.put(i, s);
      i += 1;
    }
  }

  @Override
  public void beginWindow(long windowId)
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
    if (count.isConnected() && generatedTupleCount > 0) {
      long elapsedTime = System.currentTimeMillis() - windowStartTime;
      if (elapsedTime == 0) {
        elapsedTime = 1; // prevent from / zero
      }

      long tcount = generatedTupleCount;
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
      tuples.put(OPORT_COUNT_TUPLE_COUNT, new Long(tcount));
      tuples.put(OPORT_COUNT_TUPLE_TIME, new Long(elapsedTime));
      tuples.put(OPORT_COUNT_TUPLE_TUPLES_PERSEC, new Long((tcount * 1000) / elapsedTime));
      tuples.put(OPORT_COUNT_TUPLE_WINDOWID, new Integer(count_windowid++));
      count.emit(tuples);
    }

    if (--maxCountOfWindows == 0) {
      LOG.info("reached maxCountOfWindows, interrupting thread.");
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void teardown()
  {
  }
  
 /**
 * Maximum number of Windows for this operation to run. 
 * @param i
 */
  public void setMaxCountOfWindows(int i)
  {
    maxCountOfWindows = i;
  }

  public String getKeysHelper()
  {
    return keysHelper;
  }

  /**
   * Comma separated strings which can be used as keys
   * @param value
   */
  public void setKeysHelper(String keys)
  {
    LOG.debug("in key setter");
    this.keysHelper = keys;
    keysArray = keysHelper.split(",");
  }

  public String getWeightsHelper()
  {
    return weightsHelper;
  }

  /**
   * Comma separated values which are used as weight for the same indexed keys.
   * @param value
   */
  public void setWeightsHelper(String weight)
  {
    if (weight.isEmpty()) {
      weightsArray = null;
    }
    else {
      weightsArray = weight.split(",");
    }
  }

  public String getValuesHelper()
  {
    return valuesHelper;
  }

  /**
   * Comma separated strings which can be used as values
   * @param value
   */
  public void setValuesHelper(String value)
  {
    if (value.isEmpty()) {
      valuesArray = null;
    }
    else {
      valuesArray = value.split(",");
    }
  }


  /**
   * @param tuples_blast the tuples_blast to set
   */
  public void setTuplesBlast(int tuples_blast)
  {
    this.tuples_blast = tuples_blast;
  }

  /**
   * The Rolling Window count for averaging across these many windows
   * @param r the rolling_window_count for averaging across these many windows
   */
  public void setRollingWindowCount(int r)
  {
    this.rolling_window_count = r;
  }

  @Override
  public void emitTuples()
  {
    int j = 0;

    for (int i = tuples_blast; i-- > 0;) {
      if (weights != null) { // weights are not even
        int rval = random.nextInt(total_weight);
        j = 0; // for randomization, need to reset to 0
        int wval = 0;
        for (Integer e: weights) {
          wval += e;
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
      generatedTupleCount++;

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

}
