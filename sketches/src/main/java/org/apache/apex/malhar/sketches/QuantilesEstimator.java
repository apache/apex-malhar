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
package org.apache.apex.malhar.sketches;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.yahoo.sketches.quantiles.QuantilesSketch;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * An implementation of BaseOperator that computes a "sketch" (a representation of the probability distribution using
 * a low memory footprint) of the incoming numeric data, and evaluates/outputs the cumulative distribution function and
 * quantiles of the probability distribution. Leverages the quantiles sketch implementation from the Yahoo Datasketches
 * Library.
 * <p/>
 * <b>Input Port(s) : </b><br>
 * <b>data : </b> Data values input port. <br>
 * <br>
 * <b>Output Port(s) : </b> <br>
 * <b>cdfOutput : </b>cumulative distribution function output port. <br>
 * <b>quantilesOutput : </b>quantiles output port. <br>
 * <b>pmfOutput : </b>probability mass function output port. <br>
 * <br>
 * <b>Partitions : </b> No unifier. Merging these sketches is non-trivial. <br>
 * <br>+
 */
@Evolving
@OperatorAnnotation(partitionable = false)
public class QuantilesEstimator extends BaseOperator
{

  private transient QuantilesSketch quantilesSketch = QuantilesSketch.builder().build();

  /**
   * This field determines the specific quantiles to be calculated. For a stream of numbers, the quantile at a value
   * 0 <= p <= 1 is the number x such that a fraction p of the numbers in the sorted stream are less than x. E.g., the
   * quantile at p = 0.5 is the median (half the numbers in the stream are less than the median).
   * The default is set to compute the standard quartiles (4-quantiles).
   */
  private double[] fractions = {0.0, 0.25, 0.50, 0.75, 1.00};
  /**
   * This field determines the intervals on which the probability mass function is computed.
   */
  private double[] pmfIntervals = {};


  public final transient DefaultInputPort<Double> data = new DefaultInputPort<Double>()
  {
    @Override
    public void process(Double input)
    {
      processTuple(input);
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


  /**
   * Constructor that allows non-default initialization of the quantile sketch object
   *
   * @param k:    Parameter that determines accuracy and memory usage of quantile sketch.
   * See <a href="QuantilesSketch documentation">http://datasketches.github.io/docs/Quantiles/QuantilesOverview.html</a>
   * for details.
   * @param seed: The quantile sketch algorithm is inherently random. Set seed to 0 for reproducibility in testing, but
   *              do not set otherwise.
   */
  public QuantilesEstimator(int k, short seed)
  {
    quantilesSketch = QuantilesSketch.builder().setSeed(seed).build(k);
  }

  public QuantilesEstimator() {}

  protected void processTuple(Double input)
  {
    {
      quantilesSketch.update(input);

      if (quantilesOutput.isConnected()) {
        /**
         * Computes and emits quantiles of the stream seen thus far
         */
        quantilesOutput.emit(quantilesSketch.getQuantiles(fractions));
      }

      if (cdfOutput.isConnected()) {
        /**
         * Emits (estimate of the) cumulative distribution function evaluated at the input value, according to the
         * sketched probability distribution of the stream seen thus far.
         * The use of a length-1 array looks ugly, but the getCDF method belongs to the external QuantilesSketch class
         * and is retained unchanged.
         */
        cdfOutput.emit(quantilesSketch.getCDF(new double[]{input})[0]);
      }

      if (pmfOutput.isConnected()) {
        /**
         * Emits probability mass function computed on the specified intervals. The PMF on an interval [x1, x2] is the
         * fraction of stream elements that are >= x1 and <= x2.
         */
        pmfOutput.emit(quantilesSketch.getPMF(pmfIntervals));
      }
    }
  }

  /**
   * @return sketch-size parameter k
   */
  public int getK()
  {
    return quantilesSketch.getK();
  }

  /**
   * @return quantiles being calculated
   */
  public double[] getFractions()
  {
    return fractions;
  }

  /**
   * @param fractions set quantiles to be calculated
   */
  public void setFractions(double[] fractions)
  {
    this.fractions = fractions;
  }

  /**
   * @return intervals on which probability mass function is computed
   */
  public double[] getPmfIntervals()
  {
    return pmfIntervals;
  }

  /**
   * @param pmfIntervals set intervals on which probability mass function is to be computed
   */
  public void setPmfIntervals(double[] pmfIntervals)
  {
    this.pmfIntervals = pmfIntervals;
  }

  /**
   * @return number of elements in the stream thus far
   */
  public long streamLength()
  {
    return quantilesSketch.getN();
  }

}
