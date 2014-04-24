/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.apps.etl.MapAggregator.MapAggregateEvent;
import com.datatorrent.lib.db.DataStoreWriter;

/**
 *
 */
public class AggregationsOperator extends AggregationsOperatorBase<MapAggregateEvent, DataStoreWriter<MapAggregateEvent>>
{
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
        computeMetrics(aggregateEvent, event);
      }
    }
    else {
      computeMetrics(aggregateEvent, event);
    }

    cache.setValueFor(aggregateEvent, aggregateEvent);
    updateStore(aggregateEvent);

    output.emit(aggregateEvent);
  }

  private void computeMetrics(MapAggregateEvent aggregateEvent, MapAggregateEvent event)
  {
    for (Metric metric : aggregators[event.getAggregatorIndex()].metrics) {
      Object result = metric.operation.compute(aggregateEvent.getMetric(metric.destinationKey), event.getMetric(metric.destinationKey));
      aggregateEvent.putMetric(metric.destinationKey, result);
    }
  }
}
