/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Counts and then drops the tuple; mainly used for logging. Increments a count and writes the net number (rate) to console. Useful to benchmark other modules<p>
 * This operator is neither pass through nor windowed as far as data tuples are concerned. The logging is done during end of window call.<br>
 * <b>Port</b>:<br>
 * <b>data</b>: expects K<br>
 * <br>
 * <b>Properties</b>:
 * rollingwindowcount: Number of windows to average over. Results are written to the log<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for DevNullCounter&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 1250 Million tuples/s</td><td>No tuple is emitted</td><td>In-bound rate is the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=Integer)</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for DevNullCounter&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>No Outbound port</th></tr>
 * <tr><th><i>data</i>(K)</th><th><s>No Port</s></th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>66</td><td>N/A</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */

public class DevNullCounter<K> extends BaseOperator
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    /**
     * Process each tuple. Expects upstream node to compute number of tuples in that window and send it as an int<br>
     * @param tuple
     */
    @Override
    public void process(K tuple)
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

  private boolean debug = true;

  /**
   * getter function for debug state
   * @return debug state
   */
  public boolean getDebug()
  {
    return debug;
  }

  /**
   * setter function for debug state
   * @param i sets debug to i
   */
  public void setDebug(boolean i)
  {
    debug = i;
  }

  @Min(1)
  private int rollingwindowcount = 1;

  public void setRollingwindowcount(int val) {
    rollingwindowcount = val;
  }

  /**
   * Sets up all the config parameters. Assumes checking is done and has passed
   * @param context
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
    if (!debug) {
      return;
    }
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
