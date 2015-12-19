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
package com.datatorrent.lib.complex;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Name;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * A base implementation of an operator that abstracts away the input and output port.
 *
 * Subclasses should provide the implementation to process a tuple of type I and return a tuple of
 * type O such that the return value from each input port will emit from every output port.
 *
 * <p>
 * <b>Input Port(s) :</b><br>
 * <b> input[] :</b> input port list with a default of two ports. <br>
 * <br>
 * <b>Output Port(s) :</b><br>
 * <b> output[] :</b> output port list with a default of two ports. <br>
 * <br>
 * <b>Stateful : No</b>, all state is handled through the implementing class. <br>
 * <b>Partitions : Yes</b>, no dependency among input tuples. <br>
 * <br>
 * @displayName All Way Multiple Input & Output
 * @category Complex Operators
 * @tags complex, multiple input, multiple output
 * @param <I> type being received from the input port
 * @param <O> type being sent from the output port
 * @since 3.3.0
 */
@Stateless
@Name("all-way-multiple-input-output")
public abstract class AllWayMultiInputOutput<I, O> extends BaseOperator
{
  protected static final int DEFAULT_NUM_INPUTS = 2;
  protected static final int DEFAULT_NUM_OUTPUTS = 2;

  protected transient List<DefaultInputPort<I>> inputs = null;
  protected transient List<DefaultOutputPort<O>> outputs = null;

  public AllWayMultiInputOutput() throws InstantiationException
  {
    this(DEFAULT_NUM_INPUTS, DEFAULT_NUM_OUTPUTS);
  }

  public AllWayMultiInputOutput(int numInputs, int numOutputs) throws InstantiationException
  {
    if (numInputs < 1 || numInputs > 65535) {
      throw new InstantiationException("Cannot instantiate with " + numInputs +
          "; must be 1 < numInputs < 65535");
    } else if (numOutputs < 1 || numOutputs > 65535) {
      throw new InstantiationException("Cannot instantiate with " + numOutputs +
          "; must be 1 < numOutputs < 65535");
    } else {
      inputs = new ArrayList<DefaultInputPort<I>>(numInputs);
      outputs = new ArrayList<DefaultOutputPort<O>>(numOutputs);

      for (int i = 0; i < numOutputs; ++i) {
        outputs.add(new DefaultOutputPort<O>());
      }

      for (int i = 0; i < numInputs; ++i) {
        inputs.add(new DefaultInputPort<I>() {
          @Override
          public void process(I inputTuple)
          {
            O result;

            if ((result = AllWayMultiInputOutput.this.process(inputTuple)) != null) {
              for (DefaultOutputPort<O> output : outputs) {
                output.emit(result);
              }
            }
          }
        });
      }
    }
  }

  public abstract O process(I inputTuple);
}
