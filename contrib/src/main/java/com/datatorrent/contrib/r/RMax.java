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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.rosuda.REngine.REngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.util.BaseNumberValueOperator;

/**
 * Emits at end of window, a maximum of all values sub-classed from Number in the incoming stream. <br>
 * A vector is created in the process method. This vector is passed to the 'max' operator in R in the 'endWindow'
 * function. <br>
 * <b>StateFull :</b>Yes, max value is computed over application windows. <br>
 * <b>Partitions :</b>Yes, operator is kin unifier operator. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>max</b>: emits V extends Number<br>
 * <br>
 * <br>
 **/

public class RMax<V extends Number> extends BaseNumberValueOperator<Number> implements Unifier<Number>
{
  private List<Number> numList = new ArrayList<Number>();

  private static Logger log = LoggerFactory.getLogger(RMax.class);
  REngineConnectable connectable;

  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<Number> data = new DefaultInputPort<Number>() {
    /**
     * Adds the tuple to the numList
     */
    @Override
    public void process(Number tuple)
    {
      RMax.this.process(tuple);
    }
  };

  /**
   * Adds the received tuple to the numList
   */

  @Override
  public void process(Number tuple)
  {
    numList.add(tuple);
  }

  @OutputPortFieldAnnotation(name = "max")
  public final transient DefaultOutputPort<Number> max = new DefaultOutputPort<Number>() {
    @Override
    public Unifier<Number> getUnifier()
    {
      return RMax.this;
    }
  };

  /*
   * Initialize the R engine
   */
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

  /**
   * Invokes the 'max' function from R to emit a maximum from the values received in the application window. Clears the
   * numList at the end of the function.
   **/

  @Override
  public void endWindow()
  {

    if (numList.size() == 0)
      return;

    double[] values = new double[numList.size()];
    for (int i = 0; i < numList.size(); i++) {
      values[i] = numList.get(i).doubleValue();
    }

    try {
      connectable.getRengine().assign("numList", values);

    } catch (REngineException e) {
      log.error("Exception: ", e);
      DTThrowable.rethrow(e);
    }

    double rMax = 0;
    try {
      rMax = connectable.getRengine().parseAndEval("max(numList)").asDouble();
      connectable.getRengine().parseAndEval("rm(list = setdiff(ls(), lsf.str()))");
    } catch (Exception e) {
      log.error("Exception: ", e);
      DTThrowable.rethrow(e);
    }

    log.debug(String.format("Max is : " + rMax));

    max.emit(rMax);
    numList.clear();

  }
}
