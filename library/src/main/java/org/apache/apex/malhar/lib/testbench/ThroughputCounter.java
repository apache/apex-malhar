/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.testbench;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This operator expects incoming tuples to be of type HashMap&lt;String, Integer&gt;.&nbsp;
 * These values are throughput per window from upstream operators.&nbsp;
 * At the end of the application window, the total and average throughput are emitted.
 * <p>
 * Benchmarks: This node has been benchmarked at over 5 million tuples/second in local/inline mode<br>
 * <b>Tuple Schema</b>
 * Each input tuple is HashMap<String, Integer><br>
 * Output tuple is a HashMap<String, Integer>, where strings are throughputs, averages etc<br>
 * <b>Port Interface</b><br>
 * <b>count</b>: Output port for emitting the results<br>
 * <b>data</b>: Input port for receiving the incoming tuple<br>
 * <br>
 * <b>Properties</b>:
 * rolling_window_count: Number of windows to average over
 * <br>
 * Compile time checks are:<br>
 * none
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Benchmarked at over 17 million tuples/second in local/in-line mode<br>
 * </p>
 * @displayName Throughput Counter
 * @category Test Bench
 * @tags count
 * @since 0.3.2
 */
public class ThroughputCounter<K, V extends Number> extends BaseOperator
{
  private static Logger log = LoggerFactory.getLogger(ThroughputCounter.class);

  /**
   * The input port which receives throughput information from upstream operators.
   */
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>()
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        tuple_count += e.getValue().longValue();
      }
    }
  };

  /**
   * The output port which emits throughput statistics.
   */
  public final transient DefaultOutputPort<HashMap<String,Number>> count = new DefaultOutputPort<HashMap<String, Number>>();

  public static final String OPORT_COUNT_TUPLE_AVERAGE = "avg";
  public static final String OPORT_COUNT_TUPLE_COUNT = "count";
  public static final String OPORT_COUNT_TUPLE_TIME = "window_time";
  public static final String OPORT_COUNT_TUPLE_TUPLES_PERSEC = "tuples_per_sec";
  public static final String OPORT_COUNT_TUPLE_WINDOWID = "window_id";

  private long windowStartTime = 0;
  @Min(1)
  private int rolling_window_count = 1;
  long[] tuple_numbers = null;
  long[] time_numbers = null;
  int tuple_index = 0;
  int count_denominator = 1;
  long count_windowid = 0;
  long tuple_count = 1; // so that the first begin window starts the count down
  boolean didemit = false;


  @Min(1)
  public int getRollingWindowCount()
  {
    return rolling_window_count;
  }

  public void setRollingWindowCount(int i)
  {
    rolling_window_count = i;
  }

  @Override
  public void setup(OperatorContext context)
  {
    windowStartTime = System.currentTimeMillis();
    log.debug(String.format("\nTupleCounter: set window to %d", rolling_window_count));
    if (rolling_window_count != 1) { // Initialized the tuple_numbers
      tuple_numbers = new long[rolling_window_count];
      time_numbers = new long[rolling_window_count];
      for (int i = tuple_numbers.length; i > 0; i--) {
        tuple_numbers[i - 1] = 0;
        time_numbers[i - 1] = 0;
      }
      tuple_index = 0;
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (tuple_count != 0) { // Do not restart time if no tuples were sent
      windowStartTime = System.currentTimeMillis();
      if (didemit) {
        tuple_count = 0;
      }
    }
  }

  /**
   * convenient method for not sending more than configured number of windows.
   */
  @Override
  public void endWindow()
  {
    if (tuple_count == 0) {
      return;
    }

    long elapsedTime = System.currentTimeMillis() - windowStartTime;
    if (elapsedTime == 0) {
      didemit = false;
      return;
    }

    long average;
    long tuples_per_sec = (tuple_count * 1000) / elapsedTime; // * 1000 as elapsedTime is in millis
    if (rolling_window_count == 1) {
      average = tuples_per_sec;
    } else { // use tuple_numbers
      long slots;
      if (count_denominator == rolling_window_count) {
        tuple_numbers[tuple_index] = tuple_count;
        time_numbers[tuple_index] = elapsedTime;
        slots = rolling_window_count;
        tuple_index++;
        if (tuple_index == rolling_window_count) {
          tuple_index = 0;
        }
      } else {
        tuple_numbers[count_denominator - 1] = tuple_count;
        time_numbers[count_denominator - 1] = elapsedTime;
        slots = count_denominator;
        count_denominator++;
      }
      long time_slot = 0;
      long numtuples = 0;
      for (int i = 0; i < slots; i++) {
        numtuples += tuple_numbers[i];
        time_slot += time_numbers[i];
      }
      average = (numtuples * 1000) / time_slot;
    }
    HashMap<String, Number> tuples = new HashMap<String, Number>();
    tuples.put(OPORT_COUNT_TUPLE_AVERAGE, new Long(average));
    tuples.put(OPORT_COUNT_TUPLE_COUNT, new Long(tuple_count));
    tuples.put(OPORT_COUNT_TUPLE_TIME, new Long(elapsedTime));
    tuples.put(OPORT_COUNT_TUPLE_TUPLES_PERSEC, new Long(tuples_per_sec));
    tuples.put(OPORT_COUNT_TUPLE_WINDOWID, new Long(count_windowid++));
    count.emit(tuples);
    didemit = true;
  }
}
