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
package org.apache.apex.malhar.contrib.misc.algo;

import java.util.Random;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import org.apache.apex.malhar.lib.util.BaseKeyOperator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

/**
 * This operator takes a stream of tuples as input, and emits each tuple with a specified probability.
 * <p>
 * Emits the tuple as per probability of pass rate out of total rate. <br>
 * <br>
 * An efficient filter to allow sample analysis of a stream. Very useful is the incoming stream has high throughput.
 * </p>
 * <p>
 * <br>
 * <b> StateFull : No, </b> tuple is processed in current window. <br>
 * <b> Partitions : Yes. </b> No state dependency among input tuples. <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects K<br>
 * <b>sample</b>: emits K<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>passrate</b>: Sample rate out of a total of totalrate. Default is 1<br>
 * <b>totalrate</b>: Total rate (divisor). Default is 100<br>
 * <br>
 * <b>Specific compile time checks are</b>: None<br>
 * passrate is positive integer<br>
 * totalrate is positive integer<br>
 * passrate and totalrate are not compared (i.e. passrate &lt; totalrate) check is not done to allow users to make this operator a passthrough (all) during testing<br>
 * <br>
 * <b>Specific run time checks are</b>: None<br>
 * <br>
 * </p>
 *
 * @displayName Sampler
 * @category Stats and Aggregations
 * @tags filter
 *
 * @since 0.3.2
 * @deprecated
 */
@Deprecated
@Stateless
@OperatorAnnotation(partitionable = true)
public class Sampler<K> extends BaseKeyOperator<K>
{
  /**
   * This is the input port which receives tuples.
   */
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    /**
     * Emits tuples at a rate corresponding to the given samplingPercentage.
     */
    @Override
    public void process(K tuple)
    {
      double val = random.nextDouble();
      if (val > samplingPercentage) {
        return;
      }
      sample.emit(cloneKey(tuple));
    }
  };

  /**
   * This is the output port which emits the sampled tuples.
   */
  public final transient DefaultOutputPort<K> sample = new DefaultOutputPort<K>();

  @Min(0)
  @Max(1)
  private double samplingPercentage = 1.0;

  private transient Random random = new Random();

  /**
   * Gets the samplingPercentage.
   * @return the samplingPercentage
   */
  public double getSamplingPercentage()
  {
    return samplingPercentage;
  }

  /**
   * The percentage of tuples to allow to pass through this operator. This percentage should be
   * a number between 0 and 1 inclusive.
   * @param samplingPercentage the samplingPercentage to set
   */
  public void setSamplingPercentage(double samplingPercentage)
  {
    this.samplingPercentage = samplingPercentage;
  }
}
