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
package com.datatorrent.lib.statistics;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.yahoo.sketches.quantiles.QuantilesSketch;

public class QuantilesEstimator extends BaseOperator {

  private boolean sendPerTuple = true;
  private transient QuantilesSketch quantilesSketch = QuantilesSketch.builder().build();

  private double[] fractions = {0.0, 0.25, 0.50, 0.75, 1.00};

  public final transient DefaultInputPort<Number> data = new DefaultInputPort<Number>() {
    @Override
    public void process(Number input) {

      double inputDouble = input.doubleValue();
      quantilesSketch.update(inputDouble);

      double[] valArr = {inputDouble};

      /**
       * Estimate of the Cumulative Distribution Function evaluated at input
       */
      double cdfValue = quantilesSketch.getCDF(valArr)[0];
      double[] quantiles = quantilesSketch.getQuantiles(fractions);

      cdfOutput.emit(cdfValue);
      quantilesOutput.emit(quantiles);
    }
  };

  public final transient DefaultOutputPort<Double> cdfOutput = new DefaultOutputPort<>();

  public final transient DefaultOutputPort<double[]> quantilesOutput = new DefaultOutputPort<>();

}
