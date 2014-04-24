/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.apps.etl.MapAggregator.MapAggregateEvent;
import com.datatorrent.lib.db.DataStoreWriter;
import javax.annotation.Nonnull;

/**
 * MapAggregateEvent aggregations operator
 */
public class AggregationsOperator extends AggregationsOperatorBase<MapAggregateEvent, DataStoreWriter<MapAggregateEvent>>
{
  @Nonnull
  protected MapAggregator[] aggregators;

  @Override
  protected void processTuple(MapAggregateEvent event)
  {
    MapAggregateEvent aggregateEvent = (MapAggregateEvent)cache.getValueFor(event);
    if (aggregateEvent == null) {
      aggregateEvent = retreiveFromStore(event);
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
    updateStore(aggregateEvent);

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
