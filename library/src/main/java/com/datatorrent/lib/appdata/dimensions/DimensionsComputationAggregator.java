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
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.google.common.base.Preconditions;

public class DimensionsComputationAggregator implements com.datatorrent.lib.statistics.DimensionsComputation.Aggregator<InputAggregateEvent, AggregateEvent>
{
  private static final long serialVersionUID = 201505181155L;

  private final DimensionsIncrementalAggregator staticAggregator;
  private final FieldsDescriptor aggregateDescriptor;

  public DimensionsComputationAggregator(DimensionsIncrementalAggregator staticAggregator,
                                         FieldsDescriptor aggregateDescriptor)
  {
    this.staticAggregator = Preconditions.checkNotNull(staticAggregator);
    this.aggregateDescriptor = Preconditions.checkNotNull(aggregateDescriptor);
  }

  @Override
  public AggregateEvent getGroup(InputAggregateEvent src, int aggregatorIndex)
  {
    AggregateEvent aggregateEvent = staticAggregator.createDest(src, aggregateDescriptor);
    aggregateEvent.setAggregatorIndex(aggregatorIndex);
    return aggregateEvent;
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
