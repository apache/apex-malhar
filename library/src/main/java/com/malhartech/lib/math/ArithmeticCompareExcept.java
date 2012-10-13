/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.Sink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "in_data". A compare function is imposed based on the property "key", "value", and "compare". If the tuple
 * passed the test, it is emitted on the output port "compare". If the tuple fails it is emitted on port "except". The comparison is done by getting double
 * value from the Number. Both output ports are optional, but at least one has to be connected<p>
 *  * This module is a pass through<br>
 * <br>
 * Ports:<br>
 * <b>in_data</b>: Input port, expects HashMap<String, Object><br>
 * <b>compare</b>: Output port, emits HashMap<String, Object> if compare function returns true<br>
 * <b>except</b>: Output port, emits HashMap<String, Object> if compare function is false<br>
 * <br>
 * Properties:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>comp<b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * Compile time checks<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <br>
 * Run time checks<br>
 * none<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Integer: ?? million tuples/s<br>
 * Double: ?? million tuples/s<br>
 * Long: ?? million tuples/s<br>
 * Short: ?? million tuples/s<br>
 * Float: ?? million tupels/s<br>
 *
 * @author amol
 */


@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = ArithmeticCompareExcept.IPORT_IN_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = ArithmeticCompareExcept.OPORT_COMPARE, type = PortAnnotation.PortType.OUTPUT),
  @PortAnnotation(name = ArithmeticCompareExcept.OPORT_EXCEPT, type = PortAnnotation.PortType.OUTPUT)
})
public class ArithmeticCompareExcept extends AbstractModule
{
  public static final String IPORT_IN_DATA = "in_data";
  public static final String OPORT_COMPARE = "compare";
  public static final String OPORT_EXCEPT = "except";
  private static Logger LOG = LoggerFactory.getLogger(ArithmeticCompareExcept.class);

  String key;
  double default_value = 0.0;
  double value = default_value;

  boolean compare_connected = false;
  boolean except_connected = false;

  enum supported_type {LTE, LT, EQ, NEQ, GT, GTE};
  supported_type default_type = supported_type.EQ;
  supported_type type = default_type;

   /**
   * The key to compare on
   *
   */
  public static final String KEY_KEY = "key";

  /**
   * The value to compare with
   *
   */
  public static final String KEY_VALUE = "value";

  /**
   * The compare function
   *
   */
  public static final String KEY_COMP = "comp";


    @Override
  public void connected(String id, Sink dagpart)
  {
    if (id.equals(OPORT_COMPARE)) {
      compare_connected = (dagpart != null);
    }
    else if (id.equals(OPORT_EXCEPT)) {
      except_connected = (dagpart != null);
    }
  }

  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    HashMap<String, Object> tuple = (HashMap<String, Object>) payload;
    Object val = tuple.get(key);
    double tvalue = 0;
    boolean errortuple = false;
    if (val != null) { // skip if key does not exist
      try {
        tvalue = Double.parseDouble(val.toString());
      }
      catch (NumberFormatException e) {
        errortuple = true;
      }
      if (!errortuple) {
        if (((type == supported_type.LT) && (tvalue < value))
                || ((type == supported_type.LTE) && (tvalue <= value))
                || ((type == supported_type.EQ) && (tvalue == value))
                || ((type == supported_type.NEQ) && (tvalue != value))
                || ((type == supported_type.GT) && (tvalue > value))
                || ((type == supported_type.GTE) && (tvalue >= value))) {
          if (compare_connected) {
            emit(payload);
          }
        }
        else if (except_connected) {
          emit(payload);
        }
      }
      else { // emit error tuple, the string has to be Double

      }
    }
    else { // is this an error condition?
      ;
    }
  }

  public boolean myValidation(ModuleConfiguration config)
  {
    boolean ret = true;

    String key = config.get(KEY_KEY, "");
    String vstr = config.get(KEY_VALUE, "");
    String cstr = config.get(KEY_COMP, "");

    if (key.isEmpty()) {
      ret = false;
      throw new IllegalArgumentException(String.format("Property \"%s\" has to be specified", KEY_KEY));
    }

    try {
      double value = Double.parseDouble(vstr);
    }
    catch (NumberFormatException e) {
      ret = false;
      throw new IllegalArgumentException(String.format("Property \"%s\" is not a valid number", KEY_VALUE, vstr));
    }

    if (!cstr.isEmpty() && !cstr.equals("lt") && !cstr.equals("lte") && !cstr.equals("eq") && !cstr.equals("neq") && !cstr.equals("gt") && !cstr.equals("gte")) {
      ret = false;
      throw new IllegalArgumentException(String.format("Property \"%s\" is \"%s\". Valid values are \"lte\", \"lt\", \"eq\", \"neq\", \"gt\", \"gte\"", KEY_COMP, cstr));
    }
    return ret;
  }
   /**
   *
   * @param config
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("validation failed");
    }

    key = config.get(KEY_KEY);
    String vstr = config.get(KEY_VALUE);
    String cstr = config.get(KEY_COMP, "lt");

    double value = Double.parseDouble(vstr);
    if (cstr.equals("lt")) {
      type = supported_type.LT;
    }
    else if (cstr.equals("lte")) {
      type = supported_type.LTE;
    }
    else if (cstr.equals("eq")) {
      type = supported_type.EQ;
    }
    else if (cstr.equals("neq")) {
      type = supported_type.NEQ;
    }
    else if (cstr.equals("gt")) {
      type = supported_type.GT;
    }
    else if (cstr.equals("gte")) {
      type = supported_type.GTE;
    }
    else {
      type = supported_type.EQ;
    }
    LOG.debug(String.format("Set up: \"%s\" \"%s\" \"%s\"", key, cstr, value));
  }


  /**
   *
   * Checks for user specific configuration values<p>
   *
   * @param config
   * @return boolean
   */
  @Override
  public boolean checkConfiguration(ModuleConfiguration config)
  {
    boolean ret = true;
    // TBD
    return ret && super.checkConfiguration(config);
  }
}
