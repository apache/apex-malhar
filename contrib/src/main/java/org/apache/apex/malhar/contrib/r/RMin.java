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

import org.apache.apex.malhar.lib.util.BaseNumberValueOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * Emits at the end of window, a minimum of all values sub-classed from Number in the incoming stream. <br>
 * A vector is created in the process method. This vector is passed to the 'min' function in R in the 'endWindow'
 * function. <br>
 * <b>StateFull :</b>Yes, min value is computed over application windows. <br>
 * <b>Partitions :</b>Yes, operator is kin unifier operator. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>min</b>: emits V extends Number<br>
 * <br>
 * <br>
 *
 * @displayName R Min
 * @category Scripting
 * @tags script, r
 * @since 2.1.0
 **/

public class RMin<V extends Number> extends BaseNumberValueOperator<Number> implements Unifier<Number>
{
  private List<Number> numList = new ArrayList<Number>();
  REngineConnectable connectable;

  private static Logger log = LoggerFactory.getLogger(RMin.class);

  public final transient DefaultInputPort<Number> data = new DefaultInputPort<Number>()
  {
    /**
     * Each tuple is compared to the min and a new min (if so) is stored.
     */
    @Override
    public void process(Number tuple)
    {
      RMin.this.process(tuple);
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

  public final transient DefaultOutputPort<Number> min = new DefaultOutputPort<Number>()
  {
    @Override
    public Unifier<Number> getUnifier()
    {
      return RMin.this;
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
   * Invokes the 'min' function from R to emit a maximum from the values received in the application window. Clears the
   * numList at the end of the function.
   **/
  @Override
  public void endWindow()
  {
    if (numList.size() == 0) {
      return;
    }

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

    double rMin = 0;
    try {
      rMin = connectable.getRengine().parseAndEval("min(numList)").asDouble();
      connectable.getRengine().parseAndEval("rm(list = setdiff(ls(), lsf.str()))");
    } catch (Exception e) {
      log.error("Exception: ", e);
      DTThrowable.rethrow(e);
    }

    log.debug(String.format("Min is : " + rMin));

    min.emit(rMin);
    numList.clear();

  }
}
