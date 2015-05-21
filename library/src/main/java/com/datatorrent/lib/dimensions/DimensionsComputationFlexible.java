/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.dimensions;

import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import gnu.trove.strategy.HashingStrategy;

public class DimensionsComputationFlexible
{
  public DimensionsComputationFlexible()
  {
  }
  
  private static class DirectHashingStrategy implements HashingStrategy<InputEvent>
  {
    private static final long serialVersionUID = 201505200426L;

    @Override
    public int computeHashCode(InputEvent inputEvent)
    {
      return inputEvent.getEventKey().hashCode();
    }

    @Override
    public boolean equals(InputEvent inputEventA, InputEvent inputEventB)
    {
      return inputEventA.getEventKey().equals(inputEventB.getEventKey());
    }
  }
}
