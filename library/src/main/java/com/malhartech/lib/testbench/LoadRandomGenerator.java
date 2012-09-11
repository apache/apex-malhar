/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractInputNode;
import com.malhartech.dag.NodeConfiguration;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.Sink;
import java.util.ArrayList;
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
 * This node has been benchmarked at over 10 million tuples/second for String objects in local/inline mode<br>
 * <br>
 * <b>Tuple Schema</b>: Has two choices Integer, or String<br><br>
 * <b>Port Interface</b>:It has only one output port "data" and has no input ports<br><br>
 * <b>Properties</b>:
 * <b>min_value</b> is the minimum value of the range of numbers. Default is 0<br>
 * <b>max_value</b> is the maximum value of the range of numbers. Default is 100<br>
 * <b>tuples_per_sec</b> is the upper limit of number of tuples per sec. The default value is 10000. This library node has been benchmarked at over 10 million tuples/sec<br>
 * <b>string_schema</b> controls the tuple schema. For string set it to "true". By default it is "false" (i.e. Integer schema)<br>
 * <br>
 * Compile time checks are:<br>
 * <b>min_value</b> has to be an integer<br>
 * <b>max_value</b> has to be an integer and has to be >= min_value<br>
 * <b>tuples_per_sec</b>If specified must be an integer<br>
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
public class LoadRandomGenerator extends AbstractInputNode {

    public static final String OPORT_DATA = "data";
    private static Logger LOG = LoggerFactory.getLogger(LoadRandomGenerator.class);
    int tuples_per_sec = 10000;
    int min_value = 0;
    int max_value = 100;
    boolean isstringschema = false;

    private Random random = new Random();
    private volatile boolean shutdown = false;
    private boolean outputConnected = false;
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
    public static final String KEY_TUPLES_PER_SEC = "tuples_per_sec";

    /**
     * If specified as "true" a String class is sent, else Integer is sent
     */
    public static final String KEY_STRING_SCHEMA = "string_schema";


    /**
     *
     * Code to be moved to a proper base method name
     * @param config
     * @return boolean
     */
    public boolean myValidation(NodeConfiguration config) {
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

      tuples_per_sec = config.getInt(KEY_TUPLES_PER_SEC, 10000);
      if (tuples_per_sec <= 0) {
        ret = false;
        throw new IllegalArgumentException(
                String.format("tuples_per_sec (%d) has to be > 0", tuples_per_sec));
      }
      else {
        LOG.info(String.format("Using %d tuples per second", tuples_per_sec));
      }
      return ret;
    }

    /**
     * Sets up all the config parameters. Assumes checking is done and has passed
     * @param config
     */
    @Override
    public void setup(NodeConfiguration config) {
        super.setup(config);
        if (!myValidation(config)) {
            throw new IllegalArgumentException("Did not pass validation");
        }

        // myValidation sets up all the property values
        // TBD, should we setup values only after myValidation passes successfully?
    }

    /**
     *
     * To allow emit to wait till output port is connected in a deployment on Hadoop
     * @param id
     * @param dagpart
     */
    @Override
    public void connected(String id, Sink dagpart) {
        if (id.equals(OPORT_DATA)) {
            outputConnected = true;
        }
    }

    /**
     * The only way to shut down a loadGenerator. We are looking into a property based shutdown
     */
    @Override
    public void deactivate() {
        shutdown = true;
        super.deactivate();
    }

  /**
   * Generates all the tuples till shutdown (deactivate) is issued
   *
   * @param context
   */
  @Override
  public void activate(NodeContext context)
  {
    super.activate(context);

    String sval = new String();
    Integer ival = new Integer(0);
    while (!shutdown) {
      if (outputConnected) {
        // send tuples upto tuples_per_sec and then wait for 1 ms
        int range = max_value - min_value;
        int i = 0;
        while (i < tuples_per_sec) {
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
      }
      try {
        //Thread.sleep(1000);
        Thread.sleep(5); // Remove sleep if you want to blast data at huge rate
      }
      catch (InterruptedException e) {
        LOG.error("Unexpected error while sleeping for 1 s", e);
      }
    }
    LOG.info("Finished generating tuples");
  }
}
