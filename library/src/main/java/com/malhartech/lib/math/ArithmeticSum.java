/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.Sink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port "data". At end of window sums all values
 * for each key and emits them on port "sum"<p> <br> Values are stored in a
 * hash<br> This node only functions in a windowed stram application<br> Compile
 * time error processing is done on configuration parameters<br> input port
 * "data" must be connected<br> output port "sum" must be connected<br>
 * "windowed" has to be true<br> Run time error processing are emitted on _error
 * port. The errors are:<br> Value is not a Number<br>
 *
 * @author amol
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = ArithmeticSum.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = ArithmeticSum.OPORT_SUM, type = PortAnnotation.PortType.OUTPUT),
  @PortAnnotation(name = ArithmeticSum.OPORT_COUNT, type = PortAnnotation.PortType.OUTPUT)
})
public class ArithmeticSum extends AbstractModule
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_SUM = "sum";
  public static final String OPORT_COUNT = "count";
  private static Logger LOG = LoggerFactory.getLogger(ArithmeticSum.class);
  HashMap<String, Object> sum = new HashMap<String, Object>();

  boolean count_connected = false;
  boolean sum_connected = false;

   @Override
  public void connected(String id, Sink dagpart)
  {
    if (id.equals(OPORT_COUNT)) {
      count_connected = (dagpart != null);
    }
    else if (id.equals(OPORT_SUM)) {
      sum_connected = (dagpart != null);
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
      for (Map.Entry<String, Number> e: ((HashMap<String, Number>)payload).entrySet()) {
        Object hval = sum.get(e.getKey());
        if (sum_connected && !count_connected) {
          if (hval != null) {
            hval = new Double(((Number) hval).doubleValue() + e.getValue().doubleValue());
          }
          else {
            hval = new Double(e.getValue().doubleValue());
          }
        }
        else if (sum_connected && count_connected) {
          if (hval != null) {
            ((ArrayList) hval).set(0, new Double(((Number) ((ArrayList) hval).get(0)).doubleValue() + e.getValue().doubleValue()));
            ((ArrayList) hval).set(1, (Integer) ((ArrayList) hval).get(1) + 1);
          }
          else {
            hval = new ArrayList();
            ((ArrayList) hval).add(new Double(e.getValue().doubleValue()));
            ((ArrayList) hval).add(new Integer(1));
          }
        }
        else if (count_connected) {
          if (hval != null) {
            hval = ((Integer) hval) + 1;
          }
          else {
            hval = new Integer(1);
          }
        } // If neither of the above are true, emit error
        sum.put(e.getKey(), hval);
      }
  }

  public boolean myValidation(ModuleConfiguration config)
  {
    return true;
  }

    /**
   * Node only works in windowed mode. Emits all data upon end of window tuple
   */
  @Override
  public void endWindow()
  {

    // Should allow users to send each key as a separate tuple to load balance
    // This is an aggregate node, so load balancing would most likely not be needed

    HashMap<String, Object> stuples = null;
    if (sum_connected) {
      stuples = new HashMap<String, Object>();
    }
    HashMap<String, Object> ctuples = null;
    if (count_connected) {
      ctuples = new HashMap<String, Object>();
    }

    int numtuples = 0;
    int tcount = 0;
    for (Map.Entry<String, Object> e: sum.entrySet()) {
      numtuples++;
      if (sum_connected && !count_connected) {
        stuples.put(e.getKey(), e.getValue());
      }
      else if (sum_connected && count_connected) {
        stuples.put(e.getKey(), ((ArrayList) e.getValue()).get(0));
        tcount += ((Integer) ((ArrayList) e.getValue()).get(1)).intValue();
        ctuples.put(e.getKey(), ((ArrayList) e.getValue()).get(1));
      }
      else if (count_connected) {
        ctuples.put(e.getKey(), e.getValue());
      }
    }

    if ((stuples != null) && !stuples.isEmpty()) {
      emit(OPORT_SUM, stuples);
    }
    if ((ctuples != null) && !ctuples.isEmpty()) {
      emit(OPORT_COUNT, ctuples);
    }
    sum.clear();
    if (count_connected) {
      LOG.debug(String.format("\n************* ArithmeticSum: Sent out %d tuples with value of %d ***************", numtuples, tcount));
    }
  }
}
