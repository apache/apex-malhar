/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.r;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.rosuda.REngine.REngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This operator computes variance and standard deviation over incoming data using R functions <br>
 * <br>
 * <b>Input Port(s) : </b><br>
 * <b>data : </b> Received data values on this input port. <br>
 * <br>
 * <b>Output Port(s) : </b> <br>
 * <b>variance : </b>Variance value output port. <br>
 * <b>standardDeviation : </b>Std deviation value output port. <br>
 * <br>
 * <b>StateFull : Yes</b>, value are aggregated over application window. <br>
 * <b>Partitions : No</b>, no. <br>
 * <br>
 *
 * @displayName R Standard Deviation
 * @category Scripting
 * @tags script, r
 * @since 2.1.0
 */

@OperatorAnnotation(partitionable = false)
public class RStandardDeviation extends BaseOperator
{

  private List<Number> values = new ArrayList<Number>();
  REngineConnectable connectable;

  private static Logger log = LoggerFactory.getLogger(RStandardDeviation.class);
  /**
   * Input data port.
   */
  public final transient DefaultInputPort<Number> data = new DefaultInputPort<Number>()
  {
    /**
     * Computes sum and count with each tuple
     */
    @Override
    public void process(Number tuple)
    {
      values.add(tuple.doubleValue());

    }
  };

  /**
   * Variance output port
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Number> variance = new DefaultOutputPort<Number>();

  /**
   * Standard deviation output port
   */
  public final transient DefaultOutputPort<Number> standardDeviation = new DefaultOutputPort<Number>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    try {
      connectable = new REngineConnectable();
      connectable.connect();
    } catch (IOException ioe) {
      log.error("Exception: ", ioe);
      DTThrowable.rethrow(ioe);
    }
  }

  /*
   * Stop the R engine
   */
  @Override
  public void teardown()
  {
    try {
      connectable.disconnect();
    } catch (IOException ioe) {
      log.error("Exception: ", ioe);
      DTThrowable.rethrow(ioe);
    }
  }

  /*
   * Calculates and emits the values of variance and standard deviation. Clears the vector at the end of the function.
   * SO starts with new data in every application window.
   */

  @Override
  public void endWindow()
  {
    if (values.size() == 0) {
      return;
    }
    double[] vector = new double[values.size()];
    for (int i = 0; i < values.size(); i++) {
      vector[i] = values.get(i).doubleValue();
    }
    try {
      connectable.getRengine().assign("values", vector);
    } catch (REngineException e) {
      log.error("Exception: ", e);
      DTThrowable.rethrow(e);
    }

    double rStandardDeviation = 0;
    double rVariance = 0;
    try {
      rStandardDeviation = connectable.getRengine().parseAndEval("sd(values)").asDouble();
      rVariance = connectable.getRengine().parseAndEval("var(values)").asDouble();
      connectable.getRengine().parseAndEval("rm(list = setdiff(ls(), lsf.str()))");
    } catch (Exception e) {
      log.error("Exception: ", e);
      DTThrowable.rethrow(e);
    }

    variance.emit(rVariance);
    standardDeviation.emit(rStandardDeviation);

    values.clear();

  }
}
