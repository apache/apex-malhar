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

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.dimensions.AggregateEvent.InputAggregateEvent;
import com.google.common.base.Preconditions;

public class GenericAggregator implements com.datatorrent.lib.statistics.DimensionsComputation.Aggregator<InputAggregateEvent, AggregateEvent>
{
  private static final long serialVersionUID = 201505181155L;

  private DimensionsStaticAggregator staticAggregator;
  private DimensionsConversionContext conversionContext;

  public GenericAggregator(DimensionsStaticAggregator staticAggregator,
                           DimensionsConversionContext conversionContext)
  {
    this.staticAggregator = Preconditions.checkNotNull(staticAggregator);
    this.conversionContext = Preconditions.checkNotNull(conversionContext);
  }

  @Override
  public AggregateEvent getGroup(InputAggregateEvent src, int aggregatorIndex)
  {
    return staticAggregator.createDest(src, conversionContext.aggregateDescriptor);
  }

  @Override
  public void aggregate(AggregateEvent dest, InputAggregateEvent src)
  {
    staticAggregator.aggregate(dest, src);
  }

  @Override
  public void aggregate(AggregateEvent dest, AggregateEvent src)
  {
    staticAggregator.aggregateAggs(dest, src);
  }

  @Override
  public int computeHashCode(InputAggregateEvent inputAggregateEvent)
  {
    return inputAggregateEvent.getEventKey().hashCode();
  }

  @Override
  public boolean equals(InputAggregateEvent a, InputAggregateEvent b)
  {
    return a.getEventKey().equals(b.getEventKey());
  }
}
