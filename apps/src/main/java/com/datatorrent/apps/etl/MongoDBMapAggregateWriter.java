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
package com.datatorrent.apps.etl;

import java.util.Set;

import javax.annotation.Nonnull;

import com.mongodb.BasicDBObject;

import com.datatorrent.lib.db.DataStoreWriter;

import com.datatorrent.apps.etl.MapAggregator.MapAggregateEvent;
import com.datatorrent.contrib.mongodb.MongoDBConnectable;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.util.Arrays;
import org.python.constantine.ConstantSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes dimension aggregates tuples from {@link MapAggregateEvent} to mongo db,
 * updates aggregate event with new aggregates if already found in the database
 *
 */
public class MongoDBMapAggregateWriter extends MongoDBConnectable implements DataStoreWriter<MapAggregateEvent>
{
  private final String DT_IDENTIFIER = "dt_id";
  @Nonnull
  private String table;
  @Nonnull
  private MapAggregator[] aggregators;

  @Override
  public void process(MapAggregateEvent tuple)
  {
    int aggregatorIndex = tuple.getAggregatorIndex();
    Set<String> dimensionKeys = aggregators[aggregatorIndex].getDimensionKeys();

    Object dt_hash = getIdentifier(tuple);

    BasicDBObject newTuple = new BasicDBObject();
    newTuple.put(DT_IDENTIFIER, dt_hash);
    BasicDBObject dimension = new BasicDBObject();
    for (String key : dimensionKeys) {
      dimension.put(key, tuple.getDimension(key));
    }
    newTuple.put(Constants.DIMENSIONS, dimension);

    DBObject existingTuple = getExistingTuple(tuple, dimensionKeys);

    if (existingTuple == null) {
      // insert tuple
      BasicDBObject metrics = getMetrics(tuple);
      newTuple.put(Constants.METRICS, metrics);
      db.getCollection(table).insert(newTuple);
    }
    else {
      // update tuple
      BasicDBObject metrics = computeMetrics(existingTuple, tuple);
      newTuple.put(Constants.METRICS, metrics);
      db.getCollection(table).update(existingTuple, newTuple);
    }
  }

  private BasicDBObject getMetrics(MapAggregateEvent tuple)
  {
    BasicDBObject metrics = new BasicDBObject();
    for (Metric metric : aggregators[tuple.getAggregatorIndex()].metrics) {
      Object metricValue = tuple.getMetric(metric.destinationKey);
      metrics.put(metric.destinationKey, metricValue);
    }

    return metrics;
  }

  private BasicDBObject computeMetrics(DBObject oldObj, MapAggregateEvent tuple)
  {
    BasicDBObject oldMetrics = (BasicDBObject)oldObj.get("metrics");
    BasicDBObject newMetrics = new BasicDBObject();
    for (Metric metric : aggregators[tuple.getAggregatorIndex()].metrics) {
      Object metricValue = metric.operation.compute(oldMetrics.get(metric.destinationKey), tuple.getMetric(metric.destinationKey));
      newMetrics.put(metric.destinationKey, metricValue);
    }

    return newMetrics;
  }

  public void setTable(String table)
  {
    this.table = table;
  }

  public void setAggregators(MapAggregator[] aggregators)
  {
    this.aggregators = aggregators;
  }

  private static final Logger logger = LoggerFactory.getLogger(MongoDBMapAggregateWriter.class);

  private Object getIdentifier(MapAggregateEvent tuple)
  {
    return tuple.hashCode();
  }

  private DBObject getExistingTuple(MapAggregateEvent tuple, Set<String> dimensionKeys)
  {
    BasicDBObject query = new BasicDBObject();
    query.put(DT_IDENTIFIER, getIdentifier(tuple));
    DBCursor find = db.getCollection(table).find(query);

    logger.info("tuple count = {}", find.count());

    if (find.hasNext()) {
      while (find.hasNext()) {
        DBObject dbObj = find.next();
        BasicDBObject dbObjDimensionKeys = (BasicDBObject)dbObj.get(Constants.DIMENSIONS);
        if (dbObjDimensionKeys.size() != dimensionKeys.size()) {
          continue;
        }
        for (String key : dimensionKeys) {
          if (!dbObjDimensionKeys.get(key).equals(tuple.getDimension(key))) {
            dbObj = null;
            break;
          }
        }

        if (dbObj != null) {
          return dbObj;
        }
      }
    }

    return null;
  }

}
