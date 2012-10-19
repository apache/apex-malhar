/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.GenericNode;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.OperatorConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". At end of window sends maximum of all values
 * for each key and emits them on port "max"<p> <br> Values are stored in a
 * hash<br> This node only functions in a windowed stram application<br> Compile
 * time error processing is done on configuration parameters<br> input port
 * "data" must be connected<br> output port "max" must be connected<br>
 * "windowed" has to be true<br> Run time error processing are emitted on _error
 * port. The errors are:<br> Value is not a supported type<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Integer: 8 million tuples/s<br>
 * Double: 8 million tuples/s<br>
 * Long: 8 million tuples/s<br>
 * Short: 8 million tuples/s<br>
 * Float: 8 million tupels/s<br>
 *
 * @author amol
 */


@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = Max.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = Max.OPORT_MAX, type = PortAnnotation.PortType.OUTPUT)
})
public class Max extends GenericNode
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_MAX = "max";
  private static Logger LOG = LoggerFactory.getLogger(Max.class);
  HashMap<String, Number> high = new HashMap<String, Number>();

  enum supported_type {INT, SHORT, LONG, DOUBLE, FLOAT};
  supported_type type;


   /**
   * Expected tuple schema. The default is int
   *
   */
  public static final String KEY_SCHEMA = "schema";

  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
       for (Map.Entry<String, Object> e: ((HashMap<String, Object>)payload).entrySet()) {
         Number tval = (Number) e.getValue(); // later on accept string
         String key = e.getKey();
         Number val = high.get(key);
         if (val == null) {
           switch (type) {
             case INT:
                 val = new Integer(tval.intValue());
                 break;
             case DOUBLE:
                  val = new Double(tval.doubleValue());
                  break;
             case LONG:
                 val = new Long(tval.longValue());
                 break;
             case SHORT:
                 val = new Short(tval.shortValue());
                 break;
             case FLOAT:
                 val = new Float(tval.floatValue());
                 break;
             default:
               break;
           }
           high.put(key, val);
         }
         else {
           boolean error = true;
           switch(type) {
             case INT:
               error = !(tval instanceof Integer);
               if (!error && (val.intValue() < tval.intValue())) { // no need to touch "low" as old val is automatic low
                 high.put(key, tval);
               }
               break;
             case DOUBLE:
               error = !(tval instanceof Double);
               if (!error && (val.doubleValue() < tval.doubleValue())) { // no need to touch "low" as old val is automatic low
                 high.put(key, tval);
               }
               break;
             case LONG:
               error = !(tval instanceof Long);
               if (!error && (val.longValue() < tval.longValue())) { // no need to touch "low" as old val is automatic low
                 high.put(key, tval);
               }
               break;
             case SHORT:
               error = !(tval instanceof Short);
               if (!error && (val.shortValue() < tval.shortValue())) { // no need to touch "low" as old val is automatic low
                 high.put(key, tval);
               }
               break;
             case FLOAT:
               error = !(tval instanceof Float);
               if (!error && (val.floatValue() < tval.floatValue())) { // no need to touch "low" as old val is automatic low
                 high.put(key, tval);
               }
               break;
             default:
               break;
           }
           // if (error) {emit on error port}
         }
      }
  }

  public boolean myValidation(OperatorConfiguration config)
  {
    return true;
  }
   /**
   *
   * @param config
   */
  @Override
  public void setup(OperatorConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("validation failed");
    }

    String str = config.get(KEY_SCHEMA, "");
    if (str.isEmpty()) {
      type = supported_type.INT;
      str = "integer";
    }
    else if (str.equals( "integer")) {
      type = supported_type.INT;
    }
    else if (str.equals("double")) {
      type = supported_type.DOUBLE;
    }
    else if (str.equals("long")) {
      type = supported_type.LONG;
    }
    else if (str.equals("short")) {
      type = supported_type.SHORT;
    }
    else if (str.equals("float")) {
      type = supported_type.FLOAT;
    }
    LOG.debug(String.format("Schema set to %s", str));
  }

  @Override
  public void beginWindow()
  {
    high.clear();
  }


  /**
   * Node only works in windowed mode. Emits all data upon end of window tuple
   */
  @Override
  public void endWindow()
  {
    if (!high.isEmpty()) {
      emit(OPORT_MAX, high);
    }
  }

  /**
   *
   * Checks for user specific configuration values<p>
   *
   * @param config
   * @return boolean
   */
  @Override
  public boolean checkConfiguration(OperatorConfiguration config)
  {
    boolean ret = true;
    // TBD
    return ret && super.checkConfiguration(config);
  }
}
