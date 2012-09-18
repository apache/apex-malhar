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
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Generates synthetic load. Creates tuples using random numbers and keeps emitting them on the output port "data"<p>
 * <br>
 * The load is generated as per config parameters. This class is mainly meant for testing nodes by creating a random number within
 * a range at a very high throughput. This node does not need to be windowed. It would just create tuple stream upto the limit set
 * by the config parameters.<br>
 * <br>
 * This node has been benchmarked at over 10 million tuples/second for String objects in local/inline mode<br>
 * <br>
 * <b>Tuple Schema</b>: Has two choices Integer, or String<br><br>
 * <b>Port Interface</b>:It has only one output port "data" and has no input ports<br><br>
 * <b>Properties</b>:
 * <b>min_value</b> is the minimum value of the range of numbers. Default is 0<br>
 * <b>max_value</b> is the maximum value of the range of numbers. Default is 100<br>
 * <b>tuples_burst</b> is the total amount of tuples sent by the node before the thread sleeps. The default value is 10000<br>
 * <b>string_schema</b> controls the tuple schema. For string set it to "true". By default it is "false" (i.e. Integer schema)<br>
 * <br>
 * Compile time checks are:<br>
 * <b>min_value</b> has to be an integer<br>
 * <b>max_value</b> has to be an integer and has to be >= min_value<br>
 * <b>tuples_burst</b>If specified must be an integer<br>
 * <b>sleep_time</b>Time for the thread to sleep before the next tuple send session. Default is 1000 milles<br>
 * <br>
 *
 * Compile time error checking includes<br>
 * <br>
 *
 * @author amol
 */
@NodeAnnotation(
        ports = {
  @PortAnnotation(name = LoadRandomGenerator.OPORT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class LoadRandomGenerator extends AbstractInputNode
{
  public static final String OPORT_DATA = "data";
  private static Logger LOG = LoggerFactory.getLogger(LoadRandomGenerator.class);

  final int sleep_time_default_value = 50;
  final int tuples_blast_default_value = 10000;

  protected volatile int tuples_blast = tuples_blast_default_value;
  protected volatile int sleep_time = sleep_time_default_value;

  int min_value = 0;
  int max_value = 100;
  boolean isstringschema = false;
  private Random random = new Random();
  private volatile boolean shutdown = false;

  /**
   * An integer specifying min_value.
   *
   */
  public static final String KEY_MIN_VALUE = "min_value";

  /**
   * An integer specifying max_value.
   */
  public static final String KEY_MAX_VALUE = "max_value";

  /**
   * The number of tuples sent out per milli second
   */
  public static final String KEY_TUPLES_BLAST = "tuples_blast";

 /**
   * The number of tuples sent out per milli second
   */
  public static final String KEY_SLEEP_TIME = "sleep_time";

  /**
   * If specified as "true" a String class is sent, else Integer is sent
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
    String minstr = config.get(KEY_MIN_VALUE, "0");
    String maxstr = config.get(KEY_MAX_VALUE, "100");
    isstringschema = config.getBoolean(KEY_STRING_SCHEMA, false);
    boolean ret = true;

    try {
      min_value = Integer.parseInt(minstr);
    }
    catch (NumberFormatException e) {
      ret = false;
      throw new IllegalArgumentException(String.format("min_value should be an integer (%s)", minstr));
    }

    try {
      max_value = Integer.parseInt(maxstr);
    }
    catch (NumberFormatException e) {
      ret = false;
      throw new IllegalArgumentException(String.format("max_value should be an integer (%s)", maxstr));
    }

    if (max_value <= min_value) {
      ret = false;
      throw new IllegalArgumentException(String.format("min_value (%s) should be < max_value(%s)", minstr, maxstr));
    }

    tuples_blast = config.getInt(KEY_TUPLES_BLAST, tuples_blast_default_value);
    if (tuples_blast <= 0) {
      ret = false;
      throw new IllegalArgumentException(
              String.format("tuples_per_sec (%d) has to be > 0", tuples_blast));
    }
    else {
      LOG.debug(String.format("Using %d tuples per second", tuples_blast));
    }

    sleep_time = config.getInt(KEY_SLEEP_TIME, sleep_time_default_value);
    if (sleep_time <= 0) {
      ret = false;
      throw new IllegalArgumentException(
              String.format("sleep_time (%d) has to be > 0", sleep_time));
    }
    else {
      LOG.debug(String.format("sleep_time is set to %d", sleep_time));
    }

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
      throw new IllegalArgumentException("Did not pass validation");
    }

    // myValidation sets up all the property values
    // TBD, should we setup values only after myValidation passes successfully?
  }

  /**
   * Generates all the tuples till shutdown (deactivate) is issued
   *
   * @param context
   */
  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void run()
  {
    String sval;
    Integer ival;
    while (true) {
      int range = max_value - min_value;
      int i = 0;
      while (i < tuples_blast) {
        int rval = min_value + random.nextInt(range);
        if (!isstringschema) {
          ival = rval;
          emit(OPORT_DATA, ival);
        }
        else {
          sval = String.valueOf(rval);
          emit(OPORT_DATA, sval);
        }
        i++;
      }
      try {
        Thread.sleep(sleep_time);
      }
      catch (InterruptedException e) {
        LOG.error("Unexpected error while sleeping for 1 s", e);
      }
    }
  }
}
