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
 * A base implementation of an operator that abstracts away the input and output ports.
 *
 * Subclasses should provide the implementation to process a tuple of type I and return a list of
 * tuples of type O such that each element in the list will be output to its corresponding output
 * port based on their equal indices (i.e. output[0] receives tuple[0], output[1] receives tuple[1],
 * etc.).
 *
 * In the event that the tuple list is smaller than the number of output ports the values are
 * deterministically emitted out to each port until the list is exhausted starting with the first
 * element. In the case where the tuple list is larger than the number of output ports only the
 * first tuples that correctly match their indices with the output ports are emitted and all else
 * are wasted.
 *
 * <p>
 * <b>Input Port :</b><br>
 * <b> input :</b> default input port. <br>
 * <br>
 * <b>Output Port(s) :</b><br>
 * <b> output[] :</b> output port list with a default of two ports. <br>
 * <br>
 * <b>Stateful : No</b>, all state is handled through the implementing class. <br>
 * <b>Partitions : Yes</b>, no dependency among input tuples. <br>
 * <br>
 * @displayName Single Input With List Output
 * @category Complex Operators
 * @tags complex, single input, list output
 * @param <I> type being received from the input port
 * @param <O> type being sent from the output port
 * @since 3.3.0
 */
@Stateless
@Name("single-input-list-output")
public abstract class SingleInputListOutput<I, O> extends BaseOperator
{
  protected static final int DEFAULT_NUM_OUTPUTS = 2;

  protected transient List<DefaultOutputPort<O>> outputs = null;

  protected final transient DefaultInputPort<I> input = new DefaultInputPort<I>() {
    @Override
    public void process(I inputTuple)
    {
      List<O> resultList = SingleInputListOutput.this.process(inputTuple);
      resultList = resultList != null ? resultList : new ArrayList<O>();

      int minOutputSize = resultList.size() < outputs.size() ?
          resultList.size() : outputs.size();

      for (int i = 0; i < minOutputSize; ++i) {
        O result;
        if ((result = resultList.get(i)) != null) {
          outputs.get(i).emit(result);
        }
      }
    }
  };

  public SingleInputListOutput(int numOutputs) throws InstantiationException
  {
    if (numOutputs < 1 || numOutputs > 65535) {
      throw new InstantiationException("Cannot instantiate with " + numOutputs +
          "; must be 1 < numOutputs < 65535");
    } else {
      this.outputs = new ArrayList<DefaultOutputPort<O>>(numOutputs);

      for (int i = 0; i < numOutputs; ++i) {
        this.outputs.add(new DefaultOutputPort<O>());
      }
    }
  }

  public SingleInputListOutput() throws InstantiationException
  {
    this(DEFAULT_NUM_OUTPUTS);
  }

  public abstract List<O> process(I inputTuple);
}
