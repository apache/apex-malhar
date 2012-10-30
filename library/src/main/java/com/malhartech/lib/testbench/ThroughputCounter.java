/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;


import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.OperatorConfiguration;
import java.util.HashMap;
import java.util.Map;

/**
 * Takes a in stream <b>data</b> as a HashMap<String, Integer> and add all integer values. On end of window this total and average is
 * emitted on output port <b>count</b>
 * <br>
 * <br>
 * Benchmarks: This node has been benchmarked at over 5 million tuples/second in local/inline mode<br>
 *
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
 *
 * @author amol
 */

public class ThroughputCounter<K> extends BaseOperator
{
  public final transient DefaultInputPort<HashMap<K, Integer>> data = new DefaultInputPort<HashMap<K, Integer>>(this)
  {
    @Override
    public void process(HashMap<K, Integer> tuple)
    {
      for (Map.Entry<K, Integer> e: tuple.entrySet()) {
        tuple_count += e.getValue().longValue();
      }
    }
  };

  public final transient DefaultOutputPort<HashMap<String,Number>> count = new DefaultOutputPort<HashMap<String, Number>>(this);

  public static final String OPORT_COUNT_TUPLE_AVERAGE = "avg";
  public static final String OPORT_COUNT_TUPLE_COUNT = "count";
  public static final String OPORT_COUNT_TUPLE_TIME = "window_time";
  public static final String OPORT_COUNT_TUPLE_TUPLES_PERSEC = "tuples_per_sec";
  public static final String OPORT_COUNT_TUPLE_WINDOWID = "window_id";

  private long windowStartTime = 0;
  private int rolling_window_count_default = 1;
  private int rolling_window_count = rolling_window_count_default;
  long[] tuple_numbers = null;
  long[] time_numbers = null;
  int tuple_index = 0;
  int count_denominator = 1;
  long count_windowid = 0;
  long tuple_count = 1; // so that the first begin window starts the count down

  public void setRollingWindowCount(int i)
  {
    rolling_window_count = i;
  }

  @Override
  public void setup(OperatorConfiguration config)
  {
    windowStartTime = 0;
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
  public void beginWindow()
  {
    if (tuple_count != 0) { // Do not restart time if no tuples were sent
      windowStartTime = System.currentTimeMillis();
      tuple_count = 0;
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
      elapsedTime = 1; // prevent from / zero
    }

    long average = 0;
    long tuples_per_sec = (tuple_count * 1000) / elapsedTime; // * 1000 as elapsedTime is in millis
    if (rolling_window_count == 1) {
      average = tuples_per_sec;
    }
    else { // use tuple_numbers
      long slots;
      if (count_denominator == rolling_window_count) {
        tuple_numbers[tuple_index] = tuple_count;
        time_numbers[tuple_index] = elapsedTime;
        slots = rolling_window_count;
        tuple_index++;
        if (tuple_index == rolling_window_count) {
          tuple_index = 0;
        }
      }
      else {
        tuple_numbers[count_denominator - 1] =tuple_count;
        time_numbers[count_denominator - 1] =elapsedTime;
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
  }
}
