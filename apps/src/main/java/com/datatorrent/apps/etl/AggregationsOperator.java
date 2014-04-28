/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.etl;

import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.apps.etl.MapAggregator.MapAggregateEvent;
import com.datatorrent.lib.db.DataStoreWriter;
import javax.annotation.Nonnull;

/**
 * MapAggregateEvent aggregations operator
 */
@ShipContainingJars(classes = {com.mongodb.DB.class})
public class AggregationsOperator extends AggregationsOperatorBase<MapAggregateEvent, DataStoreWriter<MapAggregateEvent>>
{
  @Nonnull
  protected MapAggregator[] aggregators;

  @Override
  protected void processTuple(MapAggregateEvent event)
  {
    MapAggregateEvent aggregateEvent = (MapAggregateEvent)cache.getValueFor(event);
    if (aggregateEvent == null) {
      aggregateEvent = event; //retreiveFromStore(event);
      if (aggregateEvent == null) {
        aggregateEvent = event;
      }
      else {
        aggregators[event.aggregatorIndex].aggregate(aggregateEvent, event);
      }
    }
    else {
      aggregators[event.aggregatorIndex].aggregate(aggregateEvent, event);
    }

    cache.setValueFor(aggregateEvent, aggregateEvent);
    //updateStore(aggregateEvent);

    output.emit(aggregateEvent);
  }

  public void setAggregators(MapAggregator[] aggregators)
  {
    this.aggregators = aggregators;
  }

  public MapAggregator[] getAggregators()
  {
    return aggregators;
  }

}
