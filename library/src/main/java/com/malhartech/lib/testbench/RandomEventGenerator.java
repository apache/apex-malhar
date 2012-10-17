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
import java.util.HashMap;
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
 * This node has been benchmarked at over 12 million tuples/second for String objects in local/inline mode<br>
 * <br>
 * <b>Tuple Schema</b>: Has two choices Integer, or String<br><br>
 * <b>Port Interface</b>:It has only one output port "data" and has no input ports<br><br>
 * <b>Properties</b>:
 * <b>key</b> is an optional parameter, the generator sends an HashMap if key is specified<br>
 * <b>min_value</b> is the minimum value of the range of numbers. Default is 0<br>
 * <b>max_value</b> is the maximum value of the range of numbers. Default is 100<br>
 * <b>tuples_burst</b> is the total amount of tuples sent by the node before handing over control. The default
 * value is 10000. A high value does not help as if window has space the control is immediately returned for mode processing<br>
 * <b>string_schema</b> controls the tuple schema. For string set it to "true". By default it is "false" (i.e. Integer schema)<br>
 * <br>
 * Compile time checks are:<br>
 * <b>min_value</b> has to be an integer<br>
 * <b>max_value</b> has to be an integer and has to be >= min_value<br>
 * <b>tuples_burst</b>If specified must be an integer<br>
 * <br>
 *
 * Compile time error checking includes<br>
 * <br>
 *
 * @author amol
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = RandomEventGenerator.OPORT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class RandomEventGenerator extends AbstractInputModule
{
  public static final String OPORT_DATA = "data";
  private static Logger LOG = LoggerFactory.getLogger(RandomEventGenerator.class);
  final int tuples_blast_default_value = 1000;
  final int sleep_time_default_value = 100;
  final int min_value_default_value = 0;
  final int max_value_default_value = 100;

  protected int tuples_blast = tuples_blast_default_value;
  protected int sleep_time = sleep_time_default_value;
  int min_value = min_value_default_value;
  int max_value = max_value_default_value;
  boolean isstringschema = false;
  private final Random random = new Random();

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
   * If specified as "true" a String class is sent, else Integer is sent
   */
  public static final String KEY_STRING_SCHEMA = "string_schema";

  /**
   * If specified as "true" a String class is sent, else Integer is sent
   */
  public static final String KEY_SLEEP_TIME = "sleep_time";

  /**
   *
   * Code to be moved to a proper base method name
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)
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
              String.format("tuples_blast (%d) has to be > 0", tuples_blast));
    }
    else {
      LOG.debug(String.format("Using %d tuples per second", tuples_blast));
    }

    sleep_time = config.getInt(KEY_SLEEP_TIME, sleep_time_default_value);
    if (sleep_time <= 0) {
      ret = false;
      throw new IllegalArgumentException(
              String.format("sleep time  (%d) has to be > 0", sleep_time));
    }
    else {
      LOG.debug(String.format("Using %d as sleep time", sleep_time));
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
      throw new IllegalArgumentException("Did not pass validation");
    }

    isstringschema = config.getBoolean(KEY_STRING_SCHEMA, false);
    tuples_blast = config.getInt(KEY_TUPLES_BLAST, tuples_blast_default_value);
    min_value = config.getInt(KEY_MIN_VALUE, min_value_default_value);
    max_value = config.getInt(KEY_MAX_VALUE, max_value_default_value);
    sleep_time = config.getInt(KEY_SLEEP_TIME, sleep_time_default_value);
  }

  /**
   * Generates all the tuples till shutdown (deactivate) is issued
   *
   * @param context
   */
  @Override
  public void process(Object payload)
  {
    int range = max_value - min_value + 1;
    int i = 0;
    // Need to add a key, if key is provided send HashMap
    while (i < tuples_blast) {
      int rval = min_value + random.nextInt(range);
      if (!isstringschema) {
        emit(OPORT_DATA, new Integer(rval));
      }
      else {
        emit(OPORT_DATA, Integer.toString(rval));
      }
      i++;
    }
    // we want to not overwhelm the downstream nodes
    // the sleep interval should be dynamically determined:
    // desired update interval / number of phone numbers
    try {
      Thread.sleep(sleep_time);
    } catch (InterruptedException e) {

    }
  }
}
