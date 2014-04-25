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

import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.metrics2.annotation.Metric;

import com.datatorrent.lib.db.DataStoreWriter;

import com.datatorrent.apps.etl.MapAggregator.MapAggregateEvent;
import com.datatorrent.contrib.mongodb.MongoDBConnectable;

/**
 * Writes dimension aggregates tuples from {@link MapAggregateEvent} to mongo db,
 * updates aggregate event with new aggregates if already found in the database
 *
 */
public class MongoDBMapAggregateWriter extends MongoDBConnectable implements DataStoreWriter<MapAggregateEvent>
{
  @Nonnull
  private String table;
  @Nonnull
  private MapAggregator[] aggregators;
  HashMap<MapAggregateEvent, MapAggregateEvent> cache = new HashMap<MapAggregateEvent, MapAggregateEvent>();
  int batchSize = 1000;

  @Override
  public void process(MapAggregateEvent tuple)
  {
    cache.put(tuple, tuple);
    if (batchSize > 0) {
      batchSize--;
    }
    else {
      batchSize = 1000;

      for (Entry<MapAggregateEvent, MapAggregateEvent> entry : cache.entrySet()) {
        tuple = entry.getValue();
        int aggregatorIndex = tuple.getAggregatorIndex();
        Set<String> dimensionKeys = aggregators[aggregatorIndex].getDimensionKeys();

        BasicDBObject newTuple = new BasicDBObject();

        BasicDBObject dimensions = getDimensions(tuple, dimensionKeys);
        newTuple.put("dimension_size", dimensions.size());
        newTuple.put(Constants.DIMENSIONS, getDimensions(tuple, dimensionKeys));

        DBObject existingTuple = getExistingTuple(tuple, dimensionKeys);

        newTuple.put(Constants.METRICS, getMetricsFromTuple(tuple));
        if (existingTuple == null) {
          // insert tuple
          db.getCollection(table).insert(newTuple);
        }
        else {
          // update tuple
          db.getCollection(table).update(existingTuple, newTuple);
        }

      }
    }
  }

  private BasicDBObject getMetricsFromTuple(MapAggregateEvent tuple)
  {
    BasicDBObject metrics = new BasicDBObject();

    Object metricValue1 = tuple.getMetric(Constants.COUNT_DEST);
    metrics.put(Constants.COUNT_DEST, metricValue1);

    Object metricValue2 = tuple.getMetric(Constants.BYTES_SRC);
    metrics.put(Constants.BYTES_SRC, metricValue2);

    return metrics;
  }

  public void setTable(String table)
  {
    this.table = table;
  }

  public void setAggregators(MapAggregator[] aggregators)
  {
    this.aggregators = aggregators;
  }

  private DBObject getExistingTuple(MapAggregateEvent tuple, Set<String> dimensionKeys)
  {
    BasicDBObject query = new BasicDBObject();

    BasicDBObject dimensions = getDimensions(tuple, dimensionKeys);
    query.put("dimension_size", dimensions.size());
    query.put(Constants.DIMENSIONS, getDimensions(tuple, dimensionKeys));
    DBCursor find = db.getCollection(table).find(query);
    if (find.size() > 1) {
      logger.info("#ashwin find size = {}", find.size());
    }

    if (find.hasNext()) {
      return find.next();
    }

    return null;
  }

  private BasicDBObject getDimensions(MapAggregateEvent tuple, Set<String> dimensionKeys)
  {
    BasicDBObject dimensions = new BasicDBObject();
    for (String key : dimensionKeys) {
      dimensions.put(key, tuple.getDimension(key));
    }
    Object timeVal;
    if ((timeVal = tuple.getDimension(Constants.TIME_ATTR)) != null) {
      Date date = new Date((Long)timeVal);
      Object bucket = tuple.getDimension("bucket");
      dimensions.put(Constants.TIME_ATTR, date);
      dimensions.put("bucket", bucket);
    }

    return dimensions;

  }

  private static final Logger logger = LoggerFactory.getLogger(MongoDBMapAggregateWriter.class);

  @Override
  public MapAggregateEvent retreive(MapAggregateEvent tuple)
  {
    int aggregatorIndex = tuple.getAggregatorIndex();
    Set<String> dimensionKeys = aggregators[aggregatorIndex].getDimensionKeys();
    DBObject existingTuple = getExistingTuple(tuple, dimensionKeys);
    if (existingTuple == null) {
      return null;
    }
    DBObject dbMetrics = (BasicDBObject)existingTuple.get(Constants.METRICS);

    Object metricValue1 = dbMetrics.get(Constants.COUNT_DEST);
    tuple.putMetric(Constants.COUNT_DEST, metricValue1);

    Object metricValue2 = dbMetrics.get(Constants.BYTES_SRC);
    tuple.putMetric(Constants.BYTES_SRC, metricValue2);

    return tuple;
  }

}
