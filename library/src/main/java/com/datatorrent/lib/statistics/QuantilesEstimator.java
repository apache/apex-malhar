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

  /**
   * This operator computes three different quantities which are output on separate output ports. If not using any of
   * these quantities, these variables can be set to avoid unnecessary computation.
   */
  private boolean computeCdf = true;
  private boolean computeQuantiles = true;
  private boolean computePmf = true;

  private transient QuantilesSketch quantilesSketch = QuantilesSketch.builder().build();

  /**
   * Constructor that allows non-default initialization of the quantile sketch object
   * @param k: Parameter that determines accuracy and memory usage of quantile sketch. See QuantilesSketch documentation
   *         for details
   * @param seed: The quantile sketch algorithm is inherently random. Set seed to 0 for reproducibility in testing, but
   *            do not set otherwise.
   */
  public QuantilesEstimator(int k, short seed) {
    quantilesSketch = QuantilesSketch.builder().setSeed(seed).build(k);
  }

  public boolean isComputeCdf() {
    return computeCdf;
  }

  public void setComputeCdf(boolean computeCdf) {
    this.computeCdf = computeCdf;
  }

  public boolean isComputeQuantiles() {
    return computeQuantiles;
  }

  public void setComputeQuantiles(boolean computeQuantiles) {
    this.computeQuantiles = computeQuantiles;
  }

  public boolean isComputePmf() {
    return computePmf;
  }

  public void setComputePmf(boolean computePmf) {
    this.computePmf = computePmf;
  }

  public int getK() {
    return quantilesSketch.getK();
  }

  /**
   * This field determines the specific quantiles to be calculated.
   * Default is set to compute the standard quartiles.
   */
  private double[] fractions = {0.0, 0.25, 0.50, 0.75, 1.00};

  /**
   * This field determines the intervals on which the probability mass function is computed.
   */
  private double[] pmfIntervals = {};

  public double[] getFractions() {
    return fractions;
  }

  public void setFractions(double[] fractions) {
    this.fractions = fractions;
  }

  public double[] getPmfIntervals() {
    return pmfIntervals;
  }

  public void setPmfIntervals(double[] pmfIntervals) {
    this.pmfIntervals = pmfIntervals;
  }

  /**
   * Input port takes a number
   */
  public final transient DefaultInputPort<Double> data = new DefaultInputPort<Double>() {
    @Override
    public void process(Double input) {

      quantilesSketch.update(input);

      if (computeQuantiles) {
        /**
         * Computes and emit quantiles of the stream seen thus far
         */
        quantilesOutput.emit(quantilesSketch.getQuantiles(fractions));
      }

      if (computeCdf) {
        /**
         * Emit (estimate of the) cumulative distribution function evaluated at the input value, according to the
         * sketched probability distribution of the stream seen thus far.
         */
        double[] valArr = {input};
        cdfOutput.emit(quantilesSketch.getCDF(valArr)[0]);
      }

      if (computePmf) {
        pmfOutput.emit(quantilesSketch.getPMF(pmfIntervals));
      }

    }
  };

  /**
   * Output port that emits cdf estimated at the current data point
   */
  public final transient DefaultOutputPort<Double> cdfOutput = new DefaultOutputPort<>();

  /**
   * Emits quantiles of stream seen thus far
   */
  public final transient DefaultOutputPort<double[]> quantilesOutput = new DefaultOutputPort<>();

  /**
   * Emits probability masses on specified intervals
   */
  public final transient DefaultOutputPort<double[]> pmfOutput = new DefaultOutputPort<>();

}
