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

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;

import java.util.Map;

public abstract class DimensionsComputationSingleSchema<INPUT_EVENT> extends DimensionsComputation<INPUT_EVENT>
{
  public static final int DEFAULT_SCHEMA_ID = 0;

  private String eventSchemaJSON;

  public DimensionsComputationSingleSchema()
  {
  }

  public AggregatorInfo getAggregatorInfo()
  {
    return AggregatorUtils.DEFAULT_AGGREGATOR_INFO;
  }

  @Override
  public AggregateEvent[] convertInputEvent(INPUT_EVENT inputEvent)
  {
    return null;
  }

  @Override
  public DimensionsStaticAggregator getAggregator(String aggregatorName)
  {
    return null;
  }

  @Override
  public DimensionsStaticAggregator getAggregator(int aggregatorID)
  {
    return null;
  }

  @Override
  public Map<Integer, DimensionsStaticAggregator> getAggregatorIDToAggregator()
  {
    return null;
  }

  @Override
  public FieldsDescriptor getAggregateFieldsDescriptor(int schemaID, int dimensionDescriptorID, int aggregatorID)
  {
    return null;
  }

  public abstract AggregateEvent createGenericAggregateEvent(INPUT_EVENT inputEvent,
                                                             DimensionsDescriptor dd,
                                                             FieldsDescriptor keyFieldsDescriptor,
                                                             FieldsDescriptor aggregateDescriptor,
                                                             int dimensionDescriptorID,
                                                             int aggregateID);
}
