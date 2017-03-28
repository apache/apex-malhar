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
package org.apache.apex.malhar.lib.window.accumulation;

import java.util.List;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.google.common.collect.Lists;

/**
 * The CompositeAccumulation is an Accumulation which delegate task to sub accumulations.
 *
 *
 * @since 3.7.0
 */
@SuppressWarnings("rawtypes")
@Evolving
public class CompositeAccumulation<InputT> implements Accumulation<InputT, List, List>
{
  /**
   * The AccumulationTag hide the implementation and prevent invalid input parameters
   *
   */
  public static class AccumulationTag
  {
    private int index;
    private AccumulationTag(int index)
    {
      this.index = index;
    }
  }

  private List<Accumulation<InputT, Object, ?>> accumulations = Lists.newArrayList();

  /**
   * @param accumulation The sub accumulation add to the composite.
   * @return The AccumulationTag. The client can get the value of sub accumulation by returned AccumulationTag.
   */
  public AccumulationTag addAccumulation(Accumulation<InputT, Object, ?> accumulation)
  {
    accumulations.add(accumulation);
    return new AccumulationTag(accumulations.size() - 1);
  }

  /**
   *
   * @param tag The tag represents the sub accumulation, which can be got from method addAccumulation()
   * @param output The output of the composite accumulation
   * @return The output of sub accumulation.
   */
  public Object getSubOutput(AccumulationTag tag, List output)
  {
    int index = tag.index;
    return accumulations.get(index).getOutput(output.get(index));
  }

  @Override
  public List defaultAccumulatedValue()
  {
    List defaultValues = Lists.newArrayList();
    for (Accumulation accumulation : accumulations) {
      defaultValues.add(accumulation.defaultAccumulatedValue());
    }
    return defaultValues;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public List accumulate(List accumulatedValues, InputT input)
  {
    for (int index = 0; index < accumulations.size(); ++index) {
      Accumulation accumulation = accumulations.get(index);
      Object oldValue = accumulatedValues.get(index);
      Object newValue = accumulation.accumulate(oldValue, input);
      if (newValue != oldValue) {
        accumulatedValues.set(index, newValue);
      }
    }
    return accumulatedValues;
  }

  @Override
  public List merge(List accumulatedValues1, List accumulatedValues2)
  {
    for (int index = 0; index < accumulations.size(); ++index) {
      accumulatedValues1.set(index,
          accumulations.get(index).merge(accumulatedValues1.get(index), accumulatedValues2.get(index)));
    }
    return accumulatedValues1;
  }

  @Override
  public List getOutput(List accumulatedValues)
  {
    return accumulatedValues;
  }

  @Override
  public List getRetraction(List values)
  {
    return values;
  }
}
