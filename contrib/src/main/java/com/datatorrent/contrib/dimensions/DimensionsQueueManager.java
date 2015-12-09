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
import com.datatorrent.lib.appdata.query.AppDataWindowEndQueueManager;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensionalExpander;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.netlet.util.Slice;

/**
 * <p>
 * This is a QueueManager for {@link DataQueryDimensional}.
 * </p>
 * <p>
 * <b>Note:</b> This {@link DimensionsQueryExecutor} will work with {@link DimensionsStoreHDHT}
 * operators that serve data for single or multiple schemas.
 * </p>
 *
 * @since 3.1.0
 */
public class DimensionsQueueManager extends AppDataWindowEndQueueManager<DataQueryDimensional, QueryMeta>
{
  /**
   * The operator which stores the data.
   */
  @NotNull
  private final DimensionsStoreHDHT operator;
  /**
   * The schema registry from which to lookup {@link DimensionalSchema}s.
   */
  @NotNull
  private final SchemaRegistry schemaRegistry;
  @NotNull
  private DataQueryDimensionalExpander dqe = SingleDataQueryDimensionalExpander.INSTANCE;

  /**
   * Creates a {@link DimensionsQueueManager} from the given {@link DimensionsStoreHDHT} and {@link SchemaRegistry}.
   *
   * @param operator       The {@link DimensionsStoreHDHT} to issue queries against.
   * @param schemaRegistry The
   * {@link SchemaRegistry} which contains all the schemas served by the {@link DimensionsStoreHDHT}.
   */
  @Deprecated
  public DimensionsQueueManager(@NotNull DimensionsStoreHDHT operator,
      @NotNull SchemaRegistry schemaRegistry)
  {
    this.operator = Preconditions.checkNotNull(operator);
    this.schemaRegistry = Preconditions.checkNotNull(schemaRegistry);
  }

  public DimensionsQueueManager(@NotNull DimensionsStoreHDHT operator,
      @NotNull SchemaRegistry schemaRegistry,
      @NotNull DataQueryDimensionalExpander dqe)
  {
    this(operator, schemaRegistry);
    this.dqe = Preconditions.checkNotNull(dqe);
  }

  @Override
  public boolean enqueue(DataQueryDimensional query, QueryMeta queryMeta, MutableLong windowExpireCount)
  {
    //Get the schema corresponding to this query.
    DimensionalSchema schemaDimensional = (DimensionalSchema)schemaRegistry.getSchema(query.getSchemaKeys());
    DimensionalConfigurationSchema configurationSchema = schemaDimensional.getDimensionalConfigurationSchema();
    Integer dimensionsDescriptorID = configurationSchema.getDimensionsDescriptorToID().get(
        query.getDimensionsDescriptor());

    if (dimensionsDescriptorID == null) {
      //Dimension combination not found
      LOG.debug("No aggregations for keys: {}", query.getKeyFields());
      return false;
    }

    //Create query key
    FieldsDescriptor keyDescriptor = configurationSchema.getDimensionsDescriptorIDToKeyDescriptor().get(
        dimensionsDescriptorID);
    //TODO mutating this references after setting them on event keys. Should find a better way to avoid
    //object creation.
    List<GPOMutable> gpoKeys = dqe.createGPOs(query.getKeysToQueryValues(), keyDescriptor);
    List<Map<String, EventKey>> aggregatorToEventKeys = Lists.newArrayList();
    //The set of all incremental aggregations to query.
    Set<String> aggregatorNames = Sets.newHashSet();

    //loop through each different type of aggregation that is being queried.
    for (String aggregatorName : query.getFieldsAggregatable().getAggregators()) {
      if (!configurationSchema.getAggregatorRegistry().isAggregator(aggregatorName)) {
        //Check if a queried aggregation is valid.
        LOG.error(aggregatorName + " is not a valid aggregator.");
        return false;
      }

      if (configurationSchema.getAggregatorRegistry().isIncrementalAggregator(aggregatorName)) {
        //The incremental aggregations to query
        aggregatorNames.add(aggregatorName);
        continue;
      }

      //this is an OTF aggregator

      //gets the child aggregators of this otf aggregator and add it to the set of incremental aggregators to query
      aggregatorNames.addAll(
          configurationSchema.getAggregatorRegistry().getOTFAggregatorToIncrementalAggregators().get(aggregatorName));
    }

    for (GPOMutable gpoKey : gpoKeys) {
      Map<String, EventKey> aggregatorToEventKey = Maps.newHashMap();

      for (String aggregatorName : aggregatorNames) {
        //build the event key for each aggregator
        LOG.debug("querying for aggregator {}", aggregatorName);
        Integer aggregatorID = configurationSchema.getAggregatorRegistry().getIncrementalAggregatorNameToID().get(
            aggregatorName);
        EventKey eventKey = new EventKey(schemaDimensional.getSchemaID(), dimensionsDescriptorID, aggregatorID, gpoKey);
        //add the event key for each aggregator
        aggregatorToEventKey.put(aggregatorName, eventKey);
      }

      aggregatorToEventKeys.add(aggregatorToEventKey);
    }

    long bucketKey = operator.getBucketForSchema(schemaDimensional.getSchemaID());
    List<Map<String, EventKey>> eventKeys = Lists.newArrayList();
    List<Map<String, HDSQuery>> hdsQueries = Lists.newArrayList();

    if (!query.isHasTime()) {
      //query doesn't have time

      //Create the queries
      for (Map<String, EventKey> aggregatorToEventKey : aggregatorToEventKeys) {
        Map<String, HDSQuery> aggregatorToQueryMap = Maps.newHashMap();
        Map<String, EventKey> aggregatorToEventKeyMap = Maps.newHashMap();

        for (Map.Entry<String, EventKey> entry : aggregatorToEventKey.entrySet()) {
          //create the query for each event key

          String aggregatorName = entry.getKey();
          EventKey eventKey = entry.getValue();
          issueHDSQuery(eventKey,
              bucketKey,
              query,
              aggregatorToEventKeyMap,
              aggregatorToQueryMap,
              aggregatorName);
        }

        hdsQueries.add(aggregatorToQueryMap);
        eventKeys.add(aggregatorToEventKeyMap);
      }
    } else {
      //the query has time

      long endTime;
      long startTime;

      if (query.isFromTo()) {
        //If the query has from and to times

        //The from time in the query
        startTime = query.getCustomTimeBucket().roundDown(query.getFrom());
        //the to time in the query
        endTime = query.getCustomTimeBucket().roundDown(query.getTo());
      } else {
        //the query has lastnumbuckets

        long time;

        if (operator.getMaxTimestamp() == null || operator.isUseSystemTimeForLatestTimeBuckets()) {
          time = System.currentTimeMillis();
        } else {
          time = operator.getMaxTimestamp();
        }

        endTime = query.getCustomTimeBucket().roundDown(time);
        startTime = endTime - query.getCustomTimeBucket().toMillis(query.getLatestNumBuckets() - 1);
      }

      long startTimeDelta = (query.getSlidingAggregateSize() - 1) * query.getCustomTimeBucket().getNumMillis();
      startTime -= startTimeDelta;

      int timeBucketId = configurationSchema.getCustomTimeBucketRegistry().getTimeBucketId(query.getCustomTimeBucket());

      for (GPOMutable gpoKey : gpoKeys) {
        gpoKey.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, timeBucketId);
      }

      //loop through each time to query
      for (long timestamp = startTime; timestamp <= endTime; timestamp += query.getCustomTimeBucket().getNumMillis()) {
        for (Map<String, EventKey> aggregatorToEventKey : aggregatorToEventKeys) {
          Map<String, HDSQuery> aggregatorToQueryMap = Maps.newHashMap();
          Map<String, EventKey> aggregatorToEventKeyMap = Maps.newHashMap();
          //loop over aggregators
          for (Map.Entry<String, EventKey> entry : aggregatorToEventKey.entrySet()) {
            String aggregatorName = entry.getKey();
            //create event key for this query
            EventKey eventKey = entry.getValue();
            eventKey.getKey().setField(DimensionsDescriptor.DIMENSION_TIME, timestamp);
            eventKey.getKey().setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, timeBucketId);
            EventKey queryEventKey = new EventKey(eventKey);

            issueHDSQuery(queryEventKey,
                bucketKey,
                query,
                aggregatorToEventKeyMap,
                aggregatorToQueryMap,
                aggregatorName);
          }

          hdsQueries.add(aggregatorToQueryMap);
          eventKeys.add(aggregatorToEventKeyMap);
        }
      }
    }

    //Create the query meta for the query
    QueryMeta qm = new QueryMeta();
    qm.setEventKeys(eventKeys);
    qm.setHdsQueries(hdsQueries);
    return super.enqueue(query, qm, null);
  }

  /**
   * This is a helper method for issuing {@link HDSQuery}s for app data {@link DataQueryDimensional} queries.
   *
   * @param eventKey                The {@link EventKey} whose value needs to be found.
   * @param bucketKey               The HDHT bucket to issue {@link HDSQuery}s against.
   * @param query                   The original {@link DataQueryDimensional} query.
   * @param aggregatorToEventKeyMap A map from aggregators to their corresponding {@link EventKey}s for this time
   *                                bucket.
   * @param aggregatorToQueryMap    A map from aggregators to their corresponding {@link HDSQuery} for this time bucket.
   * @param aggregatorName          The name of the aggregator to issue queries for.
   */
  private void issueHDSQuery(EventKey eventKey,
      long bucketKey,
      DataQueryDimensional query,
      Map<String, EventKey> aggregatorToEventKeyMap,
      Map<String, HDSQuery> aggregatorToQueryMap,
      String aggregatorName)
  {
    Slice key = new Slice(operator.getEventKeyBytesGAE(eventKey));
    //reuse the existing HDSQuery for the given key if it exists
    HDSQuery hdsQuery = operator.getQueries().get(key);

    if (hdsQuery == null) {
      //no prexisting query, so create a new one
      hdsQuery = new HDSQuery();
      hdsQuery.bucketKey = bucketKey;
      hdsQuery.key = key;
      operator.addQuery(hdsQuery);
    } else {
      //Work around for bug in HDS???
      if (hdsQuery.result == null) {
        hdsQuery.processed = false;
      }
    }

    //get the countdown for the query
    int countDown = (int)query.getCountdown();

    if (hdsQuery.keepAliveCount < countDown) {
      //keep alive time for shared query should be max countdown
      hdsQuery.keepAliveCount = countDown;
    }

    aggregatorToEventKeyMap.put(aggregatorName, eventKey);
    aggregatorToQueryMap.put(aggregatorName, hdsQuery);
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsQueueManager.class);
}
