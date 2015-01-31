/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.generic;

import java.util.List;

/**
 * DimensionsGenerator
 *
 * @since 2.0.0
 */
public class DimensionsGenerator
{
  private EventSchema eventSchema;

  public DimensionsGenerator(EventSchema eventSchema)
  {
    this.eventSchema = eventSchema;
  }


  GenericAggregator[] generateAggregators()
  {
    if (eventSchema.dimensions == null || eventSchema.dimensions.size() == 0)
    {
      return generateAllAggregators();
    }

    int numDimensions = eventSchema.dimensions.size();
    GenericAggregator[] aggregators = new GenericAggregator[numDimensions];

    for(int i = 0; i < numDimensions; i++)
    {
      aggregators[i] = new GenericAggregator(eventSchema);
      aggregators[i].init(eventSchema.dimensions.get(i));
    }
    return aggregators;
  }


  public GenericAggregator[] generateAllAggregators()
  {

    if (eventSchema.keys.size() <= 0 ) return null;

    List<String> keys = eventSchema.genericEventKeys;

    for(String key : eventSchema.keys)
    {
      if (key.equals(eventSchema.getTimestamp()))
        continue;
      keys.add(key);
    }
    int numKeys = keys.size();
    int numDimensions = 1 << numKeys;
    GenericAggregator[] aggregators = new GenericAggregator[numDimensions];

    for(int i = 0; i < numDimensions; i++)
    {
      StringBuilder builder = new StringBuilder("time=MINUTES");
      aggregators[i] = new GenericAggregator(eventSchema);
      for(int k = 0; k < numKeys; k++)
      {
        if ((i & (1 << k)) != 0) {
          builder.append(':');
          builder.append(keys.get(k));
        }
      }
      aggregators[i].init(builder.toString());
    }

    return aggregators;
  }
}
