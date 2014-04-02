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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes dimension aggregates tuples from {@link MapAggregateEvent} to mongo db,
 * updates aggregate event with new aggregates if already found in the database
 *
 */
public class MongoDBMapAggregateWriter extends MongoDBConnectable implements DataStoreWriter<MapAggregateEvent>
{
  private final String DT_HASH = "dt_hash";
  @Nonnull
  private String table;
  @Nonnull
  private MapAggregator[] aggregators;

  @Override
  public void process(MapAggregateEvent tuple)
  {
    int aggregatorIndex = tuple.getAggregatorIndex();
    logger.info("aggregator index = {} aggregators = {}", aggregatorIndex, aggregators);
    Set<String> dimensionKeys = aggregators[aggregatorIndex].getDimensionKeys();
    MapAggregateEvent group = aggregators[aggregatorIndex].getGroup(tuple, aggregatorIndex);

    BasicDBObject doc = new BasicDBObject();
    BasicDBObject query = new BasicDBObject();

    int dt_id = group.hash;
    doc.put(DT_HASH, dt_id);
    query.put(DT_HASH, dt_id);

    for (String key : dimensionKeys) {
      doc.put(key, group.get(key));
    }

    for (Metric metric : aggregators[aggregatorIndex].metrics) {
      Object metricValue = tuple.get(metric.destinationKey);
      doc.put(metric.destinationKey, metricValue);
    }

    // overwrite document if DT_HASH value found in collection, else insert
    db.getCollection(table).update(query, doc, true, false);
    System.out.println("wrote tuple..." + doc.toString());
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
}
