/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.GenericNode;
import com.malhartech.api.FailedOperationException;
import com.malhartech.api.OperatorConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes one stream and emits exactly same tuple on both the output ports. Needed to allow separation of listeners into two streamsr<p>
 * <br>
 * <br>
 * Benchmarks: This node has been benchmarked at over ?? million tuples/second in local/inline mode<br>
 *
 * <b>Tuple Schema</b>: All tuples were treated as Object
 * <b>Port Interface</b><br>
 * <b>data</b>: Input port<br>
 * <b>out_data1</b>: Output port 1<br>
 * <b>out_data2</b>: Output port 2<br>
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
  @PortAnnotation(name = StreamDuplicater.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = StreamDuplicater.OPORT_OUT_DATA1, type = PortAnnotation.PortType.OUTPUT),
  @PortAnnotation(name = StreamDuplicater.OPORT_OUT_DATA2, type = PortAnnotation.PortType.OUTPUT)
})
public class StreamDuplicater extends GenericNode
{
  public static final String IPORT_DATA = "data";
  public static final String OPORT_OUT_DATA1 = "out_data1";
  public static final String OPORT_OUT_DATA2 = "out_data2";
  private static Logger LOG = LoggerFactory.getLogger(StreamDuplicater.class);

  final static int num_oport = 2;
  /**
   * Allows usage of StreamDuplicater in a automated way
   *
   * @param i port index
   * @return returns the proper input port name
   */

  static public String getOutputName(int i) {
    String ret = "illegal_portnumber";
    if ((i != 0) && (i <= num_oport)) {
      ret = "out_data";
      ret += Integer.toString(i);
    }
    return ret;
  }

  public int getNumOutputPorts(){
    return num_oport;
  }

  /**
   * Code to be moved to a proper base method name
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(OperatorConfiguration config)  {
    return true;
  }


  /**
   * Sets up all the config parameters. Assumes checking is done and has passed
   *
   * @param config
   */
  @Override
  public void setup(OperatorConfiguration config) throws FailedOperationException {
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }
  }

  /**
   * Emit each tuple on both the output ports
   * @param payload
   */
  @Override
  public void process(Object payload) {
    emit(OPORT_OUT_DATA1, payload);
    emit(OPORT_OUT_DATA2, payload);
  }
}
