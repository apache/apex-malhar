/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merges five streams with identical schema and emits tuples on to the output port in order<br>
 * The aim is to simply merge two streams of same schema type<p>
 * <br>
 * <br>
 * Benchmarks: This node has been benchmarked at over 18 million tuples/second in local/inline mode<br>
 *
 * <b>Tuple Schema</b>: All tuples were treated as Object
 * <b>Port Interface</b><br>
 * <b>out_data</b>: Output port for emitting tuples<br>
 * <b>in_data1</b>: Input port for receiving the 1st stream of incoming tuple<br>
 * <b>in_data2</b>: Input port for receiving the 2nd stream of incoming tuple<br>
 * <b>in_data3</b>: Input port for receiving the 3rd stream of incoming tuple<br>
 * <b>in_data4</b>: Input port for receiving the 4th stream of incoming tuple<br>
 * <b>in_data5</b>: Input port for receiving the 5th stream of incoming tuple<br>
 * <br>
 * <b>Properties</b>:
 * None
 * <br>
 * Compile time checks are:<br>
 * no checks are done. Schema check is compile/instantiation time. Not runtime
 * <br>
 *
 * @author amol
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = StreamMerger5.IPORT_IN_DATA1, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = StreamMerger5.IPORT_IN_DATA2, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = StreamMerger5.IPORT_IN_DATA3, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = StreamMerger5.IPORT_IN_DATA4, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = StreamMerger5.IPORT_IN_DATA5, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = StreamMerger5.OPORT_OUT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class StreamMerger5 extends AbstractModule
{
  public static final String IPORT_IN_DATA1 = "in_data1";
  public static final String IPORT_IN_DATA2 = "in_data2";
  public static final String IPORT_IN_DATA3 = "in_data3";
  public static final String IPORT_IN_DATA4 = "in_data4";
  public static final String IPORT_IN_DATA5 = "in_data5";
  public static final String OPORT_OUT_DATA = "out_data";
  private static Logger LOG = LoggerFactory.getLogger(StreamMerger5.class);


  final static int num_iport = 5;
  /**
   * Allows usage of StreamMerger in a automated way
   *
   * @param i port index
   * @return returns the proper input port name
   */

  static public String getInputName(int i) {
    String ret = "illegal_portnumber";
    if ((i != 0) && (i <= num_iport)) {
      ret = "in_data";
      ret += Integer.toString(i);
    }
    return ret;
  }

  public int getNumInputPorts(){
    return num_iport;
  }

  /**
   * Code to be moved to a proper base method name
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)  {
    return true;
  }


  /**
   * Sets up all the config parameters. Assumes checking is done and has passed
   *
   * @param config
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException {
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }
  }

  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload) {
    emit(OPORT_OUT_DATA, payload);
  }
}
