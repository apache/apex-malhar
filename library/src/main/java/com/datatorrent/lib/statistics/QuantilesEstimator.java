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
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.yahoo.sketches.quantiles.QuantilesSketch;


/**
 * An implementation of BaseOperator that computes a "sketch" (a representation of the probability distribution using
 * a low memory footprint) of the incoming numeric data, and evaluates/outputs the cumulative distribution function and
 * quantiles of the probability distribution. Leverages the quantiles sketch implementation from the Yahoo Datasketches
 * Library.
 * <p>
 * <b>Input Port(s) : </b><br>
 * <b>data : </b> Data values input port. <br>
 * <br>
 * <b>Output Port(s) : </b> <br>
 * <b>cdfOutput : </b>cumulative distribution function output port. <br>
 * <b>quantilesOutput : </b>quantiles output port. <br>
 * <br>
 * <b>Partitions : No</b>, no will yield wrong results. <br>
 * <br>+
 */
@OperatorAnnotation(partitionable = false)
public class QuantilesEstimator extends BaseOperator {

  private boolean sendPerTuple = true;
  private transient QuantilesSketch quantilesSketch = QuantilesSketch.builder().build();

  /**
   * This field determines the specific quantiles to be calculated.
   * Default is set to compute the standard quartiles.
   */
  private double[] fractions = {0.0, 0.25, 0.50, 0.75, 1.00};

  public double[] getFractions() {
    return fractions;
  }

  public void setFractions(double[] fractions) {
    this.fractions = fractions;
  }

  /**
   * Input port takes a number
   */
  public final transient DefaultInputPort<Number> data = new DefaultInputPort<Number>() {
    @Override
    public void process(Number input) {

      double inputDouble = input.doubleValue();
      quantilesSketch.update(inputDouble);

      double[] valArr = {inputDouble};

      /**
       * (Estimate of the) cumulative distribution function evaluated at the input value, according to the probability
       * distribution of the stream seen thus far.
       */
      double cdfValue = quantilesSketch.getCDF(valArr)[0];

      /**
       * Computes quantiles of the stream seen thus far
       */
      double[] quantiles = quantilesSketch.getQuantiles(fractions);

      cdfOutput.emit(cdfValue);
      quantilesOutput.emit(quantiles);
    }
  };

  /**
   * Output port that emits cdf estimated at the current data point
   */
  public final transient DefaultOutputPort<Double> cdfOutput = new DefaultOutputPort<>();

  /**
   *
   */
  public final transient DefaultOutputPort<double[]> quantilesOutput = new DefaultOutputPort<>();

  /**
   * Constructor for non-default initialization of the quantile sketch object
   * @param k: Parameter that determines accuracy and memory usage of quantile sketch. See QuantilesSketch documentation
   *         for details
   * @param seed: The quantile sketch algorithm is inherently random. Set seed to 0 for reproducibility in testing, but
   *            do not set otherwise.
   */
  public QuantilesEstimator(int k, short seed) {
    quantilesSketch = QuantilesSketch.builder().setSeed(seed).build(k);
  }

  public int getK() {
    return quantilesSketch.getK();
  }

}
