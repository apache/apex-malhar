/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.contrib.r;

import java.util.ArrayList;
import java.util.List;

import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngine;
import org.rosuda.REngine.REngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

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
 */

@OperatorAnnotation(partitionable = false)
public class RStandardDeviation extends BaseOperator
{

  private List<Number> values = new ArrayList<Number>();
  private transient REngine rengine;

  private static Logger log = LoggerFactory.getLogger(RStandardDeviation.class);
  /**
   * Input data port.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Number> data = new DefaultInputPort<Number>() {
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
  @OutputPortFieldAnnotation(name = "variance", optional = true)
  public final transient DefaultOutputPort<Number> variance = new DefaultOutputPort<Number>();

  /**
   * Standard deviation output port
   */
  @OutputPortFieldAnnotation(name = "standardDeviation")
  public final transient DefaultOutputPort<Number> standardDeviation = new DefaultOutputPort<Number>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    try {
      String[] args = { "--vanilla" };
      this.rengine = REngine.getLastEngine();
      if (this.rengine == null) {
        // new R-engine
        this.rengine = REngine.engineForClass("org.rosuda.REngine.JRI.JRIEngine", args, null, false);
        log.info("Creating new Rengine");
      } else {
        log.info("Got last Rengine");
      }
    } catch (Exception exc) {
      log.error("Exception: ", exc);
    }
  }

  /*
   * Stop the R engine
   */
  @Override
  public void teardown()
  {

    if (rengine != null) {
      rengine.close();
    }
  }

  /*
   * Calculates and emits the values of variance and standard deviation. Clears the vector at the end of the function.
   * SO starts with new data in every application window.
   */

  @Override
  public void endWindow()
  {

    if (values.size() == 0)
      return;

    double[] vector = new double[values.size()];
    for (int i = 0; i < values.size(); i++) {
      vector[i] = values.get(i).doubleValue();
    }
    try {
      rengine.assign("values", vector);
    } catch (REngineException e) {
      e.printStackTrace();
    }

    double rStandardDeviation = 0;
    double rVariance = 0;
    try {
      rStandardDeviation = rengine.parseAndEval("sd(values)").asDouble();
      rVariance = rengine.parseAndEval("var(values)").asDouble();
    } catch (REngineException e) {
      e.printStackTrace();
    } catch (REXPMismatchException e) {
      e.printStackTrace();
    }

    variance.emit(rVariance);
    standardDeviation.emit(rStandardDeviation);

    values.clear();

  }
}
