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

package com.datatorrent.contrib.dimensions;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.HDHTReader.HDSQuery;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.AppDataWindowEndQueueManager;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

//Query Processing Classes - Start
public class DimensionsQueryQueueManager extends AppDataWindowEndQueueManager<DataQueryDimensional, QueryMeta> {
  @NotNull
  private final DimensionsStoreHDHT operator;
  @NotNull
  private final SchemaRegistry schemaRegistry;
  private final Map<Slice, HDSQuery> queries;

  public DimensionsQueryQueueManager(@NotNull DimensionsStoreHDHT operator, @NotNull SchemaRegistry schemaRegistry)
  {
    this.operator = Preconditions.checkNotNull(operator);
    this.schemaRegistry = Preconditions.checkNotNull(schemaRegistry);
    queries = operator.getQueries();
  }

  @Override
  public boolean enqueue(DataQueryDimensional query, QueryMeta queryMeta, MutableLong windowExpireCount)
  {
    DimensionalSchema schemaDimensional = (DimensionalSchema)schemaRegistry.getSchema(query.getSchemaKeys());
    DimensionalConfigurationSchema eventSchema = schemaDimensional.getGenericEventSchema();
    Integer ddID = eventSchema.getDimensionsDescriptorToID().get(query.getDimensionsDescriptor());

    if(ddID == null) {
      LOG.debug("No aggregations for keys: {}", query.getKeyFields());
      return false;
    }

    LOG.debug("Current time stamp {}", System.currentTimeMillis());
    FieldsDescriptor dd = eventSchema.getDdIDToKeyDescriptor().get(ddID);
    GPOMutable gpoKey = query.createKeyGPO(dd);
    Map<String, EventKey> aggregatorToEventKey = Maps.newHashMap();
    Set<String> aggregatorNames = Sets.newHashSet();

    for(String aggregatorName: query.getFieldsAggregatable().getAggregators()) {
      if(!eventSchema.getAggregatorRegistry().isAggregator(aggregatorName)) {
        LOG.error(aggregatorName + " is not a valid aggregator.");
        return false;
      }
      if(eventSchema.getAggregatorRegistry().isStaticAggregator(aggregatorName)) {
        aggregatorNames.add(aggregatorName);
        continue;
      }

      aggregatorNames.addAll(eventSchema.getAggregatorRegistry().getOTFAggregatorToStaticAggregators().get(aggregatorName));
    }
    for(String aggregatorName: aggregatorNames) {
      LOG.debug("querying for aggregator {}", aggregatorName);
      Integer aggregatorID = eventSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get(aggregatorName);
      EventKey eventKey = new EventKey(schemaDimensional.getSchemaID(), ddID, aggregatorID, gpoKey);
      aggregatorToEventKey.put(aggregatorName, eventKey);
    }

    long bucketKey = operator.getBucketForSchema(schemaDimensional.getSchemaID());
    List<Map<String, EventKey>> eventKeys = Lists.newArrayList();
    List<Map<String, HDSQuery>> hdsQueries = Lists.newArrayList();

    if(!query.isHasTime()) {
      Map<String, HDSQuery> aggregatorToQueryMap = Maps.newHashMap();
      Map<String, EventKey> aggregatorToEventKeyMap = Maps.newHashMap();

      for(Map.Entry<String, EventKey> entry: aggregatorToEventKey.entrySet()) {
        String aggregatorName = entry.getKey();
        EventKey eventKey = entry.getValue();
        Slice key = new Slice(operator.getEventKeyBytesGAE(eventKey));
        HDSQuery hdsQuery = queries.get(key);

        if(hdsQuery == null) {
          hdsQuery = new HDSQuery();
          hdsQuery.bucketKey = bucketKey;
          hdsQuery.key = key;
          operator.addQuery(hdsQuery);
        }
        else {
          if(hdsQuery.result == null) {
            LOG.debug("Forcing refresh for {}", hdsQuery);
            hdsQuery.processed = false;
          }
        }

        int countDown = (int)query.getCountdown();

        if(hdsQuery.keepAliveCount < countDown) {
          hdsQuery.keepAliveCount = countDown;
        }

        aggregatorToEventKeyMap.put(aggregatorName, eventKey);
        aggregatorToQueryMap.put(aggregatorName, hdsQuery);
      }

      hdsQueries.add(aggregatorToQueryMap);
      eventKeys.add(aggregatorToEventKeyMap);
    }
    else {
      long endTime;
      long startTime;

      if(query.isFromTo()) {
        startTime = query.getTimeBucket().roundDown(query.getFrom());
        endTime = query.getTimeBucket().roundDown(query.getTo());
      }
      else {
        long time = System.currentTimeMillis();
        endTime = query.getTimeBucket().roundDown(time);
        startTime = endTime - query.getTimeBucket().getTimeUnit().toMillis(query.getLatestNumBuckets() - 1);
      }

      gpoKey.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, query.getTimeBucket().ordinal());

      for(long timestamp = startTime; timestamp <= endTime; timestamp += query.getTimeBucket().getTimeUnit().toMillis(1)) {
        Map<String, HDSQuery> aggregatorToQueryMap = Maps.newHashMap();
        Map<String, EventKey> aggregatorToEventKeyMap = Maps.newHashMap();

        for(Map.Entry<String, EventKey> entry: aggregatorToEventKey.entrySet()) {
          String aggregatorName = entry.getKey();
          EventKey eventKey = entry.getValue();
          gpoKey.setField(DimensionsDescriptor.DIMENSION_TIME, timestamp);
          gpoKey.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, query.getTimeBucket().ordinal());
          EventKey queryEventKey = new EventKey(eventKey);
          Slice key = new Slice(operator.getEventKeyBytesGAE(eventKey));
          HDSQuery hdsQuery = queries.get(key);

          if(hdsQuery == null) {
            hdsQuery = new HDSQuery();
            hdsQuery.bucketKey = bucketKey;
            hdsQuery.key = key;
            operator.addQuery(hdsQuery);
          }
          else {
            if(hdsQuery.result == null) {
              hdsQuery.processed = false;
            }
          }

          int countDown = (int)query.getCountdown();

          if(hdsQuery.keepAliveCount < countDown) {
            hdsQuery.keepAliveCount = countDown;
          }

          aggregatorToEventKeyMap.put(aggregatorName, queryEventKey);
          aggregatorToQueryMap.put(aggregatorName, hdsQuery);
        }

        hdsQueries.add(aggregatorToQueryMap);
        eventKeys.add(aggregatorToEventKeyMap);
      }
    }

    QueryMeta qm = new QueryMeta();
    qm.setEventKeys(eventKeys);
    qm.setHdsQueries(hdsQueries);
    return super.enqueue(query, qm, null);
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsQueryQueueManager.class);
}
