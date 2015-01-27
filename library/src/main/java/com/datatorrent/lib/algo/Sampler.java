/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.algo;

import java.util.Random;

import javax.validation.constraints.Min;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.lib.util.BaseKeyOperator;

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
 * @category Algorithmic
 * @tags filter
 *
 * @since 0.3.2
 */
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
     * Emits the tuple as per probability of passrate out of totalrate
     */
    @Override
    public void process(K tuple)
    {
      int fval = random.nextInt(totalrate);
      if (fval >= passrate) {
        return;
      }
      sample.emit(cloneKey(tuple));
    }
  };

  /**
   * This is the output port which emits the sampled tuples.
   */
  public final transient DefaultOutputPort<K> sample = new DefaultOutputPort<K>();

  @Min(1)
  int passrate = 1;
  @Min(1)
  int totalrate = 100;
  private transient Random random = new Random();

  /**
   * getter function for pass rate
   * @return passrate
   */
  @Min(1)
  public int getPassrate()
  {
    return passrate;
  }

  /**
   * getter function for total rate
   * @return totalrate
   */
  @Min(1)
  public int getTotalrate()
  {
    return totalrate;
  }

  /**
   * Sets pass rate
   *
   * @param val passrate is set to val
   */
  public void setPassrate(int val)
  {
    passrate = val;
  }

  /**
   * Sets total rate
   *
   * @param val total rate is set to val
   */
  public void setTotalrate(int val)
  {
    totalrate = val;
  }
}
