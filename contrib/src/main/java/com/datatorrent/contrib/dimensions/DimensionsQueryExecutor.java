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
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.QueryExecutor;
import com.datatorrent.lib.appdata.query.serde.Result;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DataResultDimensional;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.datatorrent.lib.dimensions.aggregator.AggregatorUtils;
import com.datatorrent.lib.dimensions.DimensionsEvent;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.OTFAggregator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DimensionsQueryExecutor implements QueryExecutor<DataQueryDimensional, QueryMeta, MutableLong, Result> {
  private final DimensionsStoreHDHT operator;
  private final SchemaRegistry schemaRegistry;

  public DimensionsQueryExecutor(@NotNull DimensionsStoreHDHT operator, @NotNull SchemaRegistry schemaRegistry)
  {
    this.operator = Preconditions.checkNotNull(operator, "operator");
    this.schemaRegistry = Preconditions.checkNotNull(schemaRegistry, "schema registry");
  }

  @Override
  public Result executeQuery(DataQueryDimensional query, QueryMeta qm, MutableLong queueContext)
  {
    DimensionalSchema schemaDimensional = (DimensionalSchema)schemaRegistry.getSchema(query.getSchemaKeys());
    DimensionalConfigurationSchema eventSchema = schemaDimensional.getGenericEventSchema();
    LOG.debug("Processing query {} with countdown {}", query.getId(), query.getCountdown());
    List<Map<String, GPOMutable>> keys = Lists.newArrayList();
    List<Map<String, GPOMutable>> results = Lists.newArrayList();
    List<Map<String, HDSQuery>> queries = qm.getHdsQueries();
    List<Map<String, EventKey>> eventKeys = qm.getEventKeys();
    boolean allSatisfied = true;

    for(int index = 0; index < queries.size(); index++) {
      Map<String, HDSQuery> aggregatorToQuery = queries.get(index);
      Map<String, EventKey> aggregatorToEventKey = eventKeys.get(index);
      Map<String, GPOMutable> aggregatorKeys = Maps.newHashMap();
      Map<String, GPOMutable> aggregatorResults = Maps.newHashMap();

      for(String aggregatorName: aggregatorToQuery.keySet()) {
        HDSQuery hdsQuery = aggregatorToQuery.get(aggregatorName);
        EventKey eventKey = aggregatorToEventKey.get(aggregatorName);
        DimensionsEvent gae = operator.cache.get(eventKey);

        if(gae != null) {
          LOG.debug("Retrieved from cache.");
          if(gae.getKeys() == null) {
            LOG.debug("A Keys are null and they shouldn't be");
          }
          aggregatorKeys.put(aggregatorName, gae.getKeys());
          aggregatorResults.put(aggregatorName, gae.getAggregates());
        }
        else {
          Slice keySlice = new Slice(operator.getEventKeyBytesGAE(eventKey));
          //Fix this later
          byte[] value = operator.getUncommitted(operator.getBucketForSchema(schemaDimensional.getSchemaID()), keySlice);

          if(value != null) {
            gae = operator.fromKeyValueGAE(keySlice, value);
            aggregatorKeys.put(aggregatorName, gae.getKeys());
            aggregatorResults.put(aggregatorName, gae.getAggregates());
            LOG.debug("Retrieved from uncommited");
          }
          else if(hdsQuery.result != null) {
            gae = operator.getCodec().fromKeyValue(hdsQuery.key, hdsQuery.result);

            if(gae.getKeys() == null) {
              LOG.debug("B Keys are null and they shouldn't be");
            }

            LOG.debug("Retrieved from hds");
            aggregatorKeys.put(aggregatorName, gae.getKeys());
            aggregatorResults.put(aggregatorName, gae.getAggregates());
          }
          else {
            allSatisfied = false;
          }

          if(hdsQuery.processed) {
            hdsQuery.processed = false;
          }
        }
      }

      if(!aggregatorResults.isEmpty()) {
        keys.add(aggregatorKeys);
        results.add(aggregatorResults);
      }
    }

    if(!query.getIncompleteResultOK() && !allSatisfied && queueContext.longValue() > 1L) {
      return null;
    }

    return pruneResults(keys, results, query, eventSchema, queueContext);
  }

  private Result pruneResults(List<Map<String, GPOMutable>> keys,
                              List<Map<String, GPOMutable>> results,
                              DataQueryDimensional query,
                              DimensionalConfigurationSchema eventSchema,
                              MutableLong queueContext)
  {
    List<Map<String, GPOMutable>> prunedKeys = Lists.newArrayList();
    List<Map<String, GPOMutable>> prunedResults = Lists.newArrayList();

    for(int index = 0; index < keys.size(); index++) {
      Map<String, GPOMutable> key = keys.get(index);
      Map<String, GPOMutable> value = results.get(index);
      Map<String, GPOMutable> prunedKey = Maps.newHashMap();
      Map<String, GPOMutable> prunedValue = Maps.newHashMap();

      if(key.isEmpty()) {
        continue;
      }

      GPOMutable singleKey = key.entrySet().iterator().next().getValue();
      boolean completeTimeBucket = true;

      for(String aggregatorName: query.getFieldsAggregatable().getAggregators()) {
        if(eventSchema.getAggregatorRegistry().isStaticAggregator(aggregatorName)) {
          GPOMutable valueGPO = value.get(aggregatorName);

          if(valueGPO == null) {
            completeTimeBucket = false;
            break;
          }

          prunedKey.put(aggregatorName, key.get(aggregatorName));
          prunedValue.put(aggregatorName, value.get(aggregatorName));
          continue;
        }
        List<GPOMutable> mutableResults = Lists.newArrayList();
        List<String> childAggregators = eventSchema.getAggregatorRegistry().getOTFAggregatorToStaticAggregators().get(aggregatorName);
        boolean gotAllStaticAggregators = true;

        for(String childAggregator: childAggregators) {
          GPOMutable valueGPO = value.get(childAggregator);

          if(valueGPO == null) {
            gotAllStaticAggregators = false;
            break;
          }

          mutableResults.add(valueGPO);
        }

        if(!gotAllStaticAggregators) {
          continue;
        }

        Set<String> fields = query.getFieldsAggregatable().getAggregatorToFields().get(aggregatorName);
        FieldsDescriptor fd = eventSchema.getInputValuesDescriptor().getSubset(new Fields(fields));
        OTFAggregator aggregator = eventSchema.getAggregatorRegistry().getNameToOTFAggregators().get(aggregatorName);
        FieldsDescriptor outputFd = AggregatorUtils.getOutputFieldsDescriptor(fd,
                                                                              aggregator);
        GPOMutable result = aggregator.aggregate(fd,
                                                 outputFd,
                                                 mutableResults.toArray(new GPOMutable[mutableResults.size()]));
        prunedValue.put(aggregatorName, result);
        prunedKey.put(aggregatorName, singleKey);
      }

      if(completeTimeBucket) {
        prunedKeys.add(prunedKey);
        prunedResults.add(prunedValue);
      }
    }

    return new DataResultDimensional(query, prunedKeys, prunedResults, queueContext.longValue());
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsQueryExecutor.class);
}
