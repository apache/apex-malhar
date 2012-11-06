/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.annotation.InjectConfig;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Context.OperatorContext;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a in stream <b>data</b> and just drops the tuple. Used for logging. Increments a count and writes the net number (rate) to console. Mainly to be used to benchmark other modules<p>
 * <br>
 * <br>
 * <b>Tuple Schema</b>
 * Not relevant
 * <b>Port Interface</b><br>
 * <b>data</b>: expects <Object><br>
 * <br>
 * <b>Properties</b>:
 * rollingwindowcount: Number of windows to average over
 * <br>
 * Compile time checks are:<br>
 * none
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator does >1000 million tuples/sec as all tuples are simply dumped<br>
 * @author amol
 */

public class DevNullCounter<T> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<T> data = new DefaultInputPort<T>(this)
  {
    /**
     * Process each tuple. Expects upstream node to compute number of tuples in that window and send it as an int<br>
     * @param tuple
     */
    @Override
    public void process(T tuple)
    {
      tuple_count++;
    }
  };
  private static Logger log = LoggerFactory.getLogger(DevNullCounter.class);
  private long windowStartTime = 0;
  long[] tuple_numbers = null;
  long[] time_numbers = null;
  int tuple_index = 0;
  int count_denominator = 1;
  long count_windowid = 0;
  long tuple_count = 1; // so that the first begin window starts the count down

  @Min(1)
  @InjectConfig(key = "rollingwindow")
  private int rollingwindowcount = 1;

  @Min(1)
  public void setRollingwindowcount(int val) {
    rollingwindowcount = val;
  }

  /**
   * Sets up all the config parameters. Assumes checking is done and has passed
   *
   * @param config
   */
  @Override
  public void setup(OperatorContext context)
  {
    windowStartTime = 0;
    if (rollingwindowcount != 1) { // Initialized the tuple_numbers
      tuple_numbers = new long[rollingwindowcount];
      time_numbers = new long[rollingwindowcount];
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

    long average;
    long tuples_per_sec = (tuple_count * 1000) / elapsedTime; // * 1000 as elapsedTime is in millis
    if (rollingwindowcount == 1) {
      average = tuples_per_sec;
    }
    else { // use tuple_numbers
      long slots;
      if (count_denominator == rollingwindowcount) {
        tuple_numbers[tuple_index] = tuple_count;
        time_numbers[tuple_index] = elapsedTime;
        slots = rollingwindowcount;
        tuple_index++;
        if (tuple_index == rollingwindowcount) {
          tuple_index = 0;
        }
      }
      else {
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
    log.debug(String.format("\nWindowid (%d), Time (%d ms): The rate for %d tuples is %d. This window had %d tuples_per_sec ",
                            count_windowid++, elapsedTime, tuple_count, average, tuples_per_sec));
  }
}
