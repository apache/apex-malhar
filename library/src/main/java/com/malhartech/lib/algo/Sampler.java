/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.GenericNode;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.OperatorConfiguration;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Takes a stream via input port "data" and emits sample tuple on output port out_data<p>
 * <br>
 * An efficient filter to allow sample analysis of a stream. Very useful is the incoming stream has high throughput<p>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects Object
 * <b>sample</b>: Output data port, emits Object
 * <b>Properties</b>:
 * <b>rate</b>: Samplying rate, a number between 0 to 100 (type double). Default is 1% (0.01)<br>
 *
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * <b>rate</b> has to be between 0 and 100<br>
 * <br>
 * Run time checks are:<br>
 * None<br>
 * <br> *
 * @author amol<br>
 *
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = Sampler.IPORT_DATA, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = Sampler.OPORT_SAMPLE, type = PortAnnotation.PortType.OUTPUT)
})
public class Sampler extends GenericNode
{
  private static Logger LOG = LoggerFactory.getLogger(Sampler.class);
  public static final String IPORT_DATA = "in_data1";
  public static final String OPORT_SAMPLE = "sample";

  int passrate_default_value = 1;
  int totalrate_default_value = 100;
  int passrate = passrate_default_value;
  int totalrate = totalrate_default_value;

  private Random random = new Random();

 /**
   * The group by key
   *
   */
  public static final String KEY_RATE = "rate";


  /**
   *
   * Takes in a key and an arrayIndex. ReverseIndexes the strings in the ArrayIndex
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    int fval = random.nextInt(totalrate);
    if (fval >= passrate) {
      return;
    }
    emit(payload);
  }

  /**
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(OperatorConfiguration config)
  {
    boolean ret = true;
    String ratestr = config.get(KEY_RATE);

    if (!ratestr.isEmpty()) {
      String[] twofstr = ratestr.split(",");
      if (twofstr.length != 2) {
        ret = false;
        throw new IllegalArgumentException(String.format("Parameter \"rate\" has wrong format \"%s\"", ratestr));
      }
      else {
        for (String ws: twofstr) {
          try {
            Integer.parseInt(ws);
          }
          catch (NumberFormatException e) {
            ret = false;
            throw new IllegalArgumentException(String.format("Rate string should be an integer(%s)", ws));
          }
        }
      }
    }
    return ret;
  }

  /**
   *
   * @param config
   */
  @Override
  public void setup(OperatorConfiguration config) throws FailedOperationException
  {
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }

    String[] ratestr = config.getTrimmedStrings(KEY_RATE);
    passrate = Integer.parseInt(ratestr[0]);
    totalrate = Integer.parseInt(ratestr[1]);
    LOG.debug(String.format("Rate set to %d out of %d", passrate, totalrate));
  }
}
