/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.contrib.hdht.HDHTReader.HDSQuery;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.QueryExecutor;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DataResultDimensional;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.Result;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.datatorrent.lib.dimensions.DimensionsEvent;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator;
import com.datatorrent.lib.dimensions.aggregator.OTFAggregator;
import com.datatorrent.netlet.util.Slice;

/**
 * <p>
 * This is a {@link QueryExecutor} which executes {@link DataQueryDimensional}
 * queries for a App Data enabled {@link DimensionsStoreHDHT} operator.
 * </p>
 * <p>
 * <b>Note:</b> This {@link QueryExecutor} will work with {@link DimensionStoreHDHT}
 * operators that serve data for single or multiple schemas.
 * </p>
 *
 * @since 3.1.0
 */
public class DimensionsQueryExecutor implements QueryExecutor<DataQueryDimensional, QueryMeta, MutableLong, Result>
{
  /**
   * The operator from which to retrieve data from.
   */
  private final DimensionsStoreHDHT operator;
  /**
   * The schema registry from which to lookup {@link DimensionalSchema}s.
   */
  private final SchemaRegistry schemaRegistry;

  public DimensionsQueryExecutor(@NotNull DimensionsStoreHDHT operator, @NotNull SchemaRegistry schemaRegistry)
  {
    this.operator = Preconditions.checkNotNull(operator, "operator");
    this.schemaRegistry = Preconditions.checkNotNull(schemaRegistry, "schema registry");
  }

  @Override
  public Result executeQuery(DataQueryDimensional query, QueryMeta qm, MutableLong queueContext)
  {
    //Retrieving the appropriate DimensionalSchema for the given query
    DimensionalSchema schemaDimensional = (DimensionalSchema)schemaRegistry.getSchema(query.getSchemaKeys());
    //The configuration schema holds all the information about how things are aggregated.
    DimensionalConfigurationSchema configurationSchema = schemaDimensional.getDimensionalConfigurationSchema();
    LOG.debug("Processing query {} with countdown {}", query.getId(), query.getCountdown());

    //The lists two lists below are parallel lists. elements at the same indices correspond to each other.
    //Each index corresponds to a time bucket
    //Each map is a map from aggregator name to the value for that aggregator

    //list of query result event keys
    List<Map<String, EventKey>> keysEventKeys = Lists.newArrayList();
    //list of query result keys each
    List<Map<String, GPOMutable>> keys = Lists.newArrayList();
    //list of query result aggregates
    List<Map<String, GPOMutable>> results = Lists.newArrayList();

    //The lists two lists below are parallel lists. elements at the same indices correspond to each other.
    //Each index corresponds to a time bucket
    //Each map is a map from aggregator name to the value for that aggregator

    //list of queries
    List<Map<String, HDSQuery>> queries = qm.getHdsQueries();
    //list of event keys
    List<Map<String, EventKey>> eventKeys = qm.getEventKeys();

    boolean allSatisfied = true;

    //loops through all of the issues HDSQueries
    for (int index = 0; index < queries.size(); index++) {
      //Get the query and keys for this time bucket
      Map<String, HDSQuery> aggregatorToQuery = queries.get(index);
      Map<String, EventKey> aggregatorToEventKey = eventKeys.get(index);

      //allocating map to hold results for this time bucket
      Map<String, EventKey> aggregatorEventKeys = Maps.newHashMap();
      Map<String, GPOMutable> aggregatorKeys = Maps.newHashMap();
      Map<String, GPOMutable> aggregatorResults = Maps.newHashMap();

      //loop over aggregators
      for (String aggregatorName : aggregatorToQuery.keySet()) {
        //Get the original query and key for this timebucket/aggregator combination
        HDSQuery hdsQuery = aggregatorToQuery.get(aggregatorName);
        EventKey eventKey = aggregatorToEventKey.get(aggregatorName);

        //See if we have the result for the query yet.

        //First check in the operator's DimensionsEvent cache
        DimensionsEvent gae = operator.cache.get(eventKey);

        if (gae != null) {
          //Result was in the cache
          LOG.debug("Retrieved from cache. {} {}", aggregatorName, gae.getEventKey());

          //Add result keys and aggregates to result maps
          aggregatorEventKeys.put(aggregatorName, gae.getEventKey());
          aggregatorKeys.put(aggregatorName, gae.getKeys());
          aggregatorResults.put(aggregatorName, gae.getAggregates());
        } else {
          //Result was not in cache

          //TODO this is inefficient
          //Check if the uncommitted HDHT cache has the data
          Slice keySlice = new Slice(operator.getEventKeyBytesGAE(eventKey));
          byte[] value = operator.getUncommitted(operator.getBucketForSchema(schemaDimensional.getSchemaID()),
              keySlice);

          if (value != null) {
            LOG.debug("Retrieved from uncommited");
            gae = operator.fromKeyValueGAE(keySlice, value);

            //Add result keys and aggregates to result maps
            aggregatorEventKeys.put(aggregatorName, gae.getEventKey());
            aggregatorKeys.put(aggregatorName, gae.getKeys());
            aggregatorResults.put(aggregatorName, gae.getAggregates());
          } else if (hdsQuery.result != null) {
            //If the uncommitted cache did not have the result, but the asynchronous HDSQuery did
            gae = operator.getCodec().fromKeyValue(hdsQuery.key, hdsQuery.result);

            LOG.debug("Retrieved from hds");
            aggregatorEventKeys.put(aggregatorName, gae.getEventKey());
            aggregatorKeys.put(aggregatorName, gae.getKeys());
            aggregatorResults.put(aggregatorName, gae.getAggregates());
          } else {
            //The result could not be found in the operator cache, uncommitted cache, or from
            //an asynchronous HDSQuery.
            allSatisfied = false;
          }

          if (hdsQuery.processed) {
            //Refresh the result of the HDSQuery if it's processed.
            hdsQuery.processed = false;
          }
        }
      }

      if (!aggregatorResults.isEmpty()) {
        //Add results to the result lists
        keysEventKeys.add(aggregatorEventKeys);
        keys.add(aggregatorKeys);
        results.add(aggregatorResults);
      }
    }

    if (!query.getIncompleteResultOK() && !allSatisfied && queueContext.longValue() > 1L) {
      //if incomplete results are not ok,
      //And all the requested results were not found
      //And the query still has time in its countdown
      //Then don't return anything.

      //Note: If the query was at the end of its countdown, we would only return the data
      //we had even if incompleteResultOK is false. This was a design decision.
      return null;
    }

    List<Map<String, GPOMutable>> rolledKeys = Lists.newArrayList();
    List<Map<String, GPOMutable>> rolledResults = Lists.newArrayList();

    applyRolling(keysEventKeys,
        keys,
        results,
        rolledKeys,
        rolledResults,
        configurationSchema,
        query);

    return pruneResults(rolledKeys, rolledResults, query, configurationSchema, queueContext);
  }

  private void applyRolling(List<Map<String, EventKey>> keysEventKeys,
      List<Map<String, GPOMutable>> keys,
      List<Map<String, GPOMutable>> results,
      List<Map<String, GPOMutable>> rolledKeys,
      List<Map<String, GPOMutable>> rolledResults,
      DimensionalConfigurationSchema configurationSchema,
      DataQueryDimensional query)
  {
    for (int offset = 0; offset < keys.size() - (query.getSlidingAggregateSize() - 1); offset++) {
      int index = offset + (query.getSlidingAggregateSize() - 1);
      Map<String, EventKey> bucketKeysEventKeys = keysEventKeys.get(index);
      Map<String, GPOMutable> bucketKeys = keys.get(index);

      Set<String> aggregators = Sets.newHashSet(bucketKeys.keySet());
      for (int rollingIndex = 0; rollingIndex < query.getSlidingAggregateSize(); rollingIndex++) {
        //Get aggregators for rolling bucket
        Map<String, GPOMutable> key = keys.get(offset + rollingIndex);
        aggregators.retainAll(key.keySet());
      }

      Set<String> unNeededAggregators = Sets.newHashSet(bucketKeys.keySet());
      unNeededAggregators.removeAll(aggregators);

      for (String unNeededAggregator : unNeededAggregators) {
        bucketKeys.remove(unNeededAggregator);
      }

      Map<String, GPOMutable> result = Maps.newHashMap();

      if (!aggregators.isEmpty()) {
        for (int rollingIndex = 0; rollingIndex < query.getSlidingAggregateSize(); rollingIndex++) {
          Map<String, GPOMutable> currentResult = results.get(offset + rollingIndex);
          for (String aggregator : aggregators) {
            IncrementalAggregator incrementalAggregator =
                configurationSchema.getAggregatorRegistry().getNameToIncrementalAggregator().get(aggregator);
            GPOMutable aggregate = result.get(aggregator);
            GPOMutable currentAggregate = currentResult.get(aggregator);
            EventKey currentEventKey = bucketKeysEventKeys.get(aggregator);

            if (aggregate == null) {
              result.put(aggregator, currentAggregate);
            } else {
              incrementalAggregator.aggregate(new Aggregate(currentEventKey, aggregate),
                  new Aggregate(currentEventKey, currentAggregate));
            }
          }
        }
      }

      rolledKeys.add(bucketKeys);
      rolledResults.add(result);
    }
  }

  /**
   * This method is responsible for pruning result lists. Pruning result lists is necessary
   * because we only want to return results that the user requested. Why would we have results
   * the user didn't request you ask? The reason is because of {@link OTFAggregator}s. When the
   * user requests an on the fly aggregation like average, two queries are made: one for sum and one
   * for count. When results are returned to the user we do not want to provide them sum and count, because
   * they only asked for average. So the sum and count results should be used to compute the average, and then
   * they should not be returned to the user. Additionally, if the user requests average, and we issue sum and
   * count queries, but only get the sum back; we should not return any result because we cannot compute the average.
   *
   * @param keys                The list of result keys.
   * @param results             The list of result aggregates.
   * @param query               The query issued.
   * @param configurationSchema The dimensional configuration schema.
   * @param queueContext        The countdown for the query.
   * @return The pruned dimensional result.
   */
  private Result pruneResults(List<Map<String, GPOMutable>> keys,
      List<Map<String, GPOMutable>> results,
      DataQueryDimensional query,
      DimensionalConfigurationSchema configurationSchema,
      MutableLong queueContext)
  {
    List<Map<String, GPOMutable>> prunedKeys = Lists.newArrayList();
    List<Map<String, GPOMutable>> prunedResults = Lists.newArrayList();

    //Loop through each time bucket for the result keys and aggregates
    for (int index = 0; index < keys.size(); index++) {
      //Results for time bucket.
      Map<String, GPOMutable> key = keys.get(index);
      Map<String, GPOMutable> value = results.get(index);

      //Pruned results for time bucket.
      Map<String, GPOMutable> prunedKey = Maps.newHashMap();
      Map<String, GPOMutable> prunedValue = Maps.newHashMap();

      if (key.isEmpty()) {
        //no data for this time bucket
        //skip this
        continue;
      }

      //get a key. all the keys for a time bucket are the same except for the aggregatorID
      //the aggregatorID is not important for this part of the code so any key will do.
      GPOMutable singleKey = key.entrySet().iterator().next().getValue();

      //loop through each aggregator.
      for (String aggregatorName : query.getFieldsAggregatable().getAggregators()) {
        if (configurationSchema.getAggregatorRegistry().isIncrementalAggregator(aggregatorName)) {
          //If the aggregator is an incremental aggregator.
          GPOMutable valueGPO = value.get(aggregatorName);

          if (valueGPO == null) {
            //this time bucket is not complete.
            break;
          }

          //add the incrementla aggregator to the list of values.
          prunedKey.put(aggregatorName, key.get(aggregatorName));
          prunedValue.put(aggregatorName, value.get(aggregatorName));
          //we are done go to the next aggregator.
          continue;
        }

        //This is an OTFAggregator

        List<GPOMutable> mutableResults = Lists.newArrayList();
        //get the child aggregators
        List<String> childAggregators =
            configurationSchema.getAggregatorRegistry().getOTFAggregatorToIncrementalAggregators().get(aggregatorName);

        boolean gotAllStaticAggregators = true;

        //Get the fields that the user queried
        Set<String> fieldsSet = query.getFieldsAggregatable().getAggregatorToFields().get(aggregatorName);
        Fields fields = new Fields(fieldsSet);

        for (String childAggregator : childAggregators) {
          //get the values for the child aggregators
          GPOMutable valueGPO = value.get(childAggregator);

          if (valueGPO == null) {
            //we don't have all the child aggregators, we can't compute the OTFAggregation
            gotAllStaticAggregators = false;
            break;
          }

          //Add the child aggregator results to the list of results
          mutableResults.add(new GPOMutable(valueGPO,
              fields));
        }

        if (!gotAllStaticAggregators) {
          //we didn't get all the incremental aggregations required to compute this OTF aggregation
          //so we must skip computing the result
          continue;
        }

        //Get the OTFAggregator
        OTFAggregator aggregator = configurationSchema.getAggregatorRegistry().getNameToOTFAggregators().get(
            aggregatorName);

        //Compute the OTF aggregation
        GPOMutable result = aggregator.aggregate(mutableResults.toArray(new GPOMutable[mutableResults.size()]));

        //Add the result to the pruned list of results
        prunedValue.put(aggregatorName, result);
        prunedKey.put(aggregatorName, singleKey);
      }

      if (prunedKey.isEmpty()) {
        continue;
      }

      //add the aggregations to the result list
      prunedKeys.add(prunedKey);
      prunedResults.add(prunedValue);
    }

    return new DataResultDimensional(query, prunedKeys, prunedResults, queueContext.longValue());
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsQueryExecutor.class);
}
