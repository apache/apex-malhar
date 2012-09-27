/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.ModuleConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes in one stream via input port <b>data</b>. The data is key, value pair. It retains the last N values on that key.
 * Output port gets the last N values. The node also provides a lookup via port <b>lookup</b>
 * <br>
 * <br>
 * <b>Schema</b>: The tuple(s) have to be HashMap<String, Object>. Strings are key and Object is data<br>
 * <br>
 * <b>Description</b>: Takes data for every key and keeps last N values. N is the value given by property <b>depth</b>
 * <br>
 * <b>Benchmarks</b>: The benchmarks are done by blasting as many HashMaps as possible on inline mode<br>
 * <br>
 * <b>Port Interface</b>:
 * data: Input data as HashMap<String, Object>. This is the key value pair that is inserted into the FIFO<br>
 * query: Special input port that allows operation of FIFO module to respond to a query. The query is a String object that is the key on which to query. Output is sent to console port<br>
 * fifo: Output of fifo for every key. Operates in two modes, sends "all" the fifo content, or just the first one. Default is the first one<br>
 * console: Sends out all the fifo contents for the String on the query port<br>
 * <br>
 * <b>Properties</b>
 * depth: Depth of the Fifo. The number of objects to be retained<br>
 * mode: Operates in either "all" or "first" mode. Default if "first". If all mode the fifo port gets all the data emitted
 *
 *
 * @author amol
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = AlgothmicFIFO.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = AlgothmicFIFO.OPORT_FIFO, type = PortAnnotation.PortType.OUTPUT),
  @PortAnnotation(name = AlgothmicFIFO.OPORT_CONSOLE, type = PortAnnotation.PortType.OUTPUT)
})
public class AlgothmicFIFO extends AbstractModule
{
  public static final String IPORT_QUERY = "query";
  public static final String IPORT_DATA = "data";
  public static final String OPORT_FIFO = "fifo";
  public static final String OPORT_CONSOLE = "console";
  private static Logger LOG = LoggerFactory.getLogger(AlgothmicFIFO.class);

  HashMap<String, Number> sum = new HashMap<String, Number>();

  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
      for (Map.Entry<String, Number> e: ((HashMap<String, Number>)payload).entrySet()) {
        Number val = sum.get(e.getKey());
        if (val != null) {
          val = new Double(val.doubleValue() + e.getValue().doubleValue());
        }
        else {
          val = new Double(e.getValue().doubleValue());
        }
        sum.put(e.getKey(), val);
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
