/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in two streams via input ports "numerator" and "denominator". At the
 * end of window computes the quotient for each key and emits the result on port
 * "quotient".<p> <br> Each stream is added to a hash. The values are added for
 * each key within the window and for each stream.<<br> This node only functions in
 * a windowed stram application<br>Currently only HashMap schema is supported (Key, Number)<br>
 * <br>
 * <br> Compile time error processing is done
 * on configuration parameters<br> property <b>multiply_by</b> has to be an
 * integer.<br>property <b>dokey</b> is a boolean. It true the node ignores the values and counts the instances of each key (i.e. value=1.0)<br>
 * <br>input ports <b>numerator</b>, <b>denominator</b> must be
 * connected.<br> outbound port <b>quotient</b> must be connected<br>
 * <br><b>All Run time errors are TBD</b><br>
 * Run time error processing are emitted on _error
 * port. The errors are:<br> Divide by zero (Error): no result is emitted on
 * "outport".<br> Input tuple not an integer on denominator stream: This tuple
 * would not be counted towards the result.<br> Input tuple not an integer on
 * numerator stream: This tuple would not be counted towards the result.<br>
 * <br>
 * Benchmarks:<br>
 * With HashMap schema the node does about 3 Million/tuples per second<br>
 * <br>
 *
 * @author amol<br>
 *
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = ArithmeticQuotient.IPORT_NUMERATOR, type = PortType.INPUT),
  @PortAnnotation(name = ArithmeticQuotient.IPORT_DENOMINATOR, type = PortType.INPUT),
  @PortAnnotation(name = ArithmeticQuotient.OPORT_QUOTIENT, type = PortType.OUTPUT)
})
public class ArithmeticQuotient extends AbstractModule
{
  public static final String IPORT_NUMERATOR = "numerator";
  public static final String IPORT_DENOMINATOR = "denominator";
  public static final String OPORT_QUOTIENT = "quotient";
  private static Logger LOG = LoggerFactory.getLogger(ArithmeticQuotient.class);


  int mult_by = 1;
  HashMap<String, Number> numerators = new HashMap<String, Number>();
  HashMap<String, Number> denominators = new HashMap<String, Number>();
  boolean dokey = false;
  /**
   * Multiplies the quotient by this number. Ease of use for percentage (*
   * 100) or CPM (* 1000)
   *
   */
  public static final String KEY_MULTIPLY_BY = "multiply_by";

  /**
   * Ignore the value and just use key to compute the quotient
   *
   */
  public static final String KEY_DOKEY = "dokey";


 /**
   *
   * @param config
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }
    mult_by = config.getInt(KEY_MULTIPLY_BY, 1);
    dokey = config.getBoolean(KEY_DOKEY, false);
    LOG.debug(String.format("Set mult_by(%d), and dokey(%s)", mult_by, dokey ? "true" : "false"));
  }

  public boolean myValidation(ModuleConfiguration config)
  {
    boolean ret = true;

    try {
      mult_by = config.getInt(KEY_MULTIPLY_BY, 1);
    }
    catch (Exception e) {
      ret = false;
      throw new IllegalArgumentException(String.format("key %s (%s) has to be an an integer",
                                                       KEY_MULTIPLY_BY, config.get(KEY_MULTIPLY_BY)));
    }
    // dokey is not checked as getBoolean always returns a value
    return ret;
  }

  @Override
  public void process(Object payload)
  {
    Map<String, Number> active;
    if (IPORT_NUMERATOR.equals(getActivePort())) {
      active = numerators;
    }
    else {
      active = denominators;
    }

    for (Map.Entry<String, Number> e: ((HashMap<String, Number>)payload).entrySet()) {
      Number val = active.get(e.getKey());
      if (val == null) {
        val = e.getValue();
      }
      else {
        if (dokey) { // skip incoming value, and simply count the occurances of the keys, (for example ctr)
          val = new Double(val.doubleValue() + 1.0);
        }
        else {
         val = new Double(val.doubleValue() + e.getValue().doubleValue());
        }
      }
      active.put(e.getKey(), val);
    }
  }

  @Override
  public void endWindow()
  {
    HashMap<String, Number> tuples = new HashMap<String, Number>();
    for (Map.Entry<String, Number> e: denominators.entrySet()) {
      Number nval = numerators.get(e.getKey());
      if (nval == null) {
        tuples.put(e.getKey(), new Double(0.0));
      }
      else {
        tuples.put(e.getKey(), new Double((nval.doubleValue() / e.getValue().doubleValue()) * mult_by));
        numerators.remove(e.getKey()); // so that all left over keys can be reported
      }
    }

    // Should allow users to send each key as a separate tuple to load balance
    // This is an aggregate node, so load balancing would most likely not be needed
    if (!tuples.isEmpty()) {
      emit(tuples);
    }
    /* Now if numerators has any keys issue divide by zero error
     for (Map.Entry<String, Number> e : numerators.entrySet()) {
     // emit error
     }
     */
    numerators.clear();
    denominators.clear();
  }
}
