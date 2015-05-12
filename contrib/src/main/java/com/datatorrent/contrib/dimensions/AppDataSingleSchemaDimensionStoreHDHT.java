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

import java.io.Serializable;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.AppData;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.appdata.dimensions.*;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent.EventKey;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import com.datatorrent.lib.appdata.qr.DataSerializerFactory;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.processor.AppDataWWEQueryQueueManager;
import com.datatorrent.lib.appdata.qr.processor.QueryComputer;
import com.datatorrent.lib.appdata.qr.processor.QueryProcessor;
import com.datatorrent.lib.appdata.schemas.*;
import java.io.IOException;

/**
 * @displayName Simple App Data Dimensions Store
 * @category Store
 * @tags appdata, dimensions, store
 */
public class AppDataSingleSchemaDimensionStoreHDHT extends DimensionsStoreHDHT implements Serializable
{
  private static final long serialVersionUID = 201503231218L;
  private static final Logger logger = LoggerFactory.getLogger(AppDataSingleSchemaDimensionStoreHDHT.class);

  public static final int SCHEMA_ID = 0;
  public static final long DEFAULT_BUCKET_ID = 0;

  @NotNull
  private String eventSchemaJSON;
  private String dimensionalSchemaJSON;

  @VisibleForTesting
  protected transient DimensionalEventSchema eventSchema;
  private transient SchemaDimensional dimensionalSchema;

  //Query Processing - Start
  private transient QueryProcessor<DataQueryDimensional, QueryMeta, MutableLong, MutableBoolean, Result> queryProcessor;
  private final transient DataDeserializerFactory queryDeserializerFactory;
  @NotNull
  private AppDataFormatter appDataFormatter = new AppDataFormatter();
  private transient SchemaRegistry schemaRegistry;
  @NotNull
  private AggregatorInfo aggregatorInfo = AggregatorUtils.DEFAULT_AGGREGATOR_INFO;
  private transient DataSerializerFactory resultSerializerFactory;
  //Query Processing - End

  private boolean updateEnumValues = false;
  @SuppressWarnings({"rawtypes"})
  private Map<String, Set<Comparable>> seenEnumValues;

  @AppData.ResultPort
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<String>();

  @InputPortFieldAnnotation(optional = true)
  @AppData.QueryPort
  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override public void process(String s)
    {
      logger.debug("Received {}", s);
      Data query;
      try {
        query = queryDeserializerFactory.deserialize(s);
      }
      catch(IOException ex) {
        logger.error("error parsing query: {}", s);
        logger.error("{}", ex);
        return;
      }

      if(query instanceof SchemaQuery) {
        dimensionalSchema.setTo(System.currentTimeMillis());

        if(updateEnumValues) {
          dimensionalSchema.setEnumsSetComparable(seenEnumValues);
        }

        SchemaResult schemaResult = schemaRegistry.getSchemaResult((SchemaQuery) query);

        if(schemaResult != null) {
          String schemaResultJSON = resultSerializerFactory.serialize(schemaResult);
          logger.info("Emitter {}", schemaResultJSON);
          queryResult.emit(schemaResultJSON);
        }
      }
      else if(query instanceof DataQueryDimensional) {
        DataQueryDimensional gdq = (DataQueryDimensional) query;
        queryProcessor.enqueue(gdq, null, null);
      }
      else {
        logger.error("Invalid query {}", s);
      }
    }
  };

  @SuppressWarnings("unchecked")
  public AppDataSingleSchemaDimensionStoreHDHT()
  {
    queryDeserializerFactory = new DataDeserializerFactory(SchemaQuery.class, DataQueryDimensional.class);
  }

  @Override
  public void processEvent(AggregateEvent gae) {
    super.processEvent(gae);

    if(updateEnumValues) {
      for(String field: gae.getKeys().getFieldDescriptor().getFields().getFields()) {
        if(DimensionsDescriptor.RESERVED_DIMENSION_NAMES.contains(field)) {
          continue;
        }

        @SuppressWarnings("rawtypes")
        Comparable fieldValue = (Comparable)gae.getKeys().getField(field);
        seenEnumValues.get(field).add(fieldValue);
      }
    }
  }

  @Override
  protected long getBucketKey(AggregateEvent event)
  {
    return AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID;
  }

  @Override
  public void setup(OperatorContext context)
  {
    aggregatorInfo.setup();

    //Setup for query processing
    queryProcessor =
    new QueryProcessor<DataQueryDimensional, QueryMeta, MutableLong, MutableBoolean, Result>(
                                                  new DimensionsQueryComputer(this),
                                                  new DimensionsQueryQueueManager(this));

    eventSchema = new DimensionalEventSchema(eventSchemaJSON,
                                             aggregatorInfo);
    dimensionalSchema = new SchemaDimensional(dimensionalSchemaJSON,
                                              eventSchema);

    //seenEnumValues

    schemaRegistry = new SchemaRegistrySingle(dimensionalSchema);
    resultSerializerFactory = new DataSerializerFactory(appDataFormatter);
    queryDeserializerFactory.setContext(DataQueryDimensional.class, schemaRegistry);
    super.setup(context);

    if(!dimensionalSchema.isFixedFromTo()) {
      dimensionalSchema.setFrom(System.currentTimeMillis());
    }

    //

    if(seenEnumValues == null) {
      seenEnumValues = Maps.newHashMap();
      for(String key: eventSchema.getAllKeysDescriptor().getFieldList()) {
        @SuppressWarnings("rawtypes")
        Set<Comparable> enumValuesSet= Sets.newHashSet();
        seenEnumValues.put(key, enumValuesSet);
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    queryProcessor.beginWindow(windowId);
    super.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();

    MutableBoolean done = new MutableBoolean(false);

    while(done.isFalse()) {
      Result aotr = queryProcessor.process(done);

      if(aotr != null) {
        String result = resultSerializerFactory.serialize(aotr);
        logger.debug("Emitting the result: {}", result);
        queryResult.emit(result);
      }
    }

    queryProcessor.endWindow();
  }

  @Override
  public void teardown()
  {
    queryProcessor.teardown();
    super.teardown();
  }

  @Override
  public DimensionsStaticAggregator getAggregator(int aggregatorID)
  {
    return aggregatorInfo.getStaticAggregatorIDToAggregator().get(aggregatorID);
  }

  @Override
  public FieldsDescriptor getKeyDescriptor(int schemaID, int dimensionsDescriptorID)
  {
    if(schemaID != 0) {
      throw new UnsupportedOperationException("Invalid schemaID: " + schemaID);
    }

    return eventSchema.getDdIDToKeyDescriptor().get(dimensionsDescriptorID);
  }

  @Override
  public FieldsDescriptor getValueDescriptor(int schemaID, int dimensionsDescriptorID, int aggregatorID)
  {
    if(schemaID != 0) {
      throw new UnsupportedOperationException("Invalid schemaID: " + schemaID);
    }

    return eventSchema.getDdIDToAggIDToOutputAggDescriptor().get(dimensionsDescriptorID).get(aggregatorID);
  }

  @Override
  public long getBucketForSchema(int schemaID)
  {
    return DEFAULT_BUCKET_ID;
  }

  @Override
  public int getPartitionGAE(AggregateEvent inputEvent)
  {
    return inputEvent.getEventKey().hashCode();}

  /**
   * @param eventSchemaJSON the eventSchemaJSON to set
   */
  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
  }

  /**
   * @param dimensionalSchemaJSON the dimensionalSchemaJSON to set
   */
  public void setDimensionalSchemaJSON(String dimensionalSchemaJSON)
  {
    this.dimensionalSchemaJSON = dimensionalSchemaJSON;
  }

  @Override
  protected int getAggregatorID(String aggregatorName)
  {
    return AggregatorStaticType.NAME_TO_ORDINAL.get(aggregatorName);
  }

  public void setAppDataFormatter(AppDataFormatter appDataFormatter)
  {
    this.appDataFormatter = appDataFormatter;
  }

  /**
   * @return the appDataFormatter
   */
  public AppDataFormatter getAppDataFormatter()
  {
    return appDataFormatter;
  }

  /**
   * @return the aggregatorInfo
   */
  public AggregatorInfo getAggregatorInfo()
  {
    return aggregatorInfo;
  }

  /**
   * @param aggregatorInfo the aggregatorInfo to set
   */
  public void setAggregatorInfo(@NotNull AggregatorInfo aggregatorInfo)
  {
    this.aggregatorInfo = aggregatorInfo;
  }

  /**
   * @return the updateEnumValues
   */
  public boolean isUpdateEnumValues()
  {
    return updateEnumValues;
  }

  /**
   * @param updateEnumValues the updateEnumValues to set
   */
  public void setUpdateEnumValues(boolean updateEnumValues)
  {
    this.updateEnumValues = updateEnumValues;
  }

  //Query Processing Classes - Start
  class DimensionsQueryQueueManager extends AppDataWWEQueryQueueManager<DataQueryDimensional, QueryMeta>
  {
    private final AppDataSingleSchemaDimensionStoreHDHT operator;

    public DimensionsQueryQueueManager(AppDataSingleSchemaDimensionStoreHDHT operator)
    {
      this.operator = operator;
    }

    @Override
    public boolean enqueue(DataQueryDimensional query, QueryMeta queryMeta, MutableLong windowExpireCount)
    {
      Integer ddID = eventSchema.getDimensionsDescriptorToID().get(query.getDd());

      if(ddID == null) {
        logger.debug("No aggregations for keys: {}", query.getKeyFields());
        return false;
      }

      logger.debug("Current time stamp {}", System.currentTimeMillis());

      FieldsDescriptor dd = eventSchema.getDdIDToKeyDescriptor().get(ddID);
      GPOMutable gpoKey = query.createKeyGPO(dd);

      Map<String, EventKey> aggregatorToEventKey = Maps.newHashMap();
      Set<String> aggregatorNames = Sets.newHashSet();

      for(String aggregatorName: query.getFieldsAggregatable().getAggregators()) {
        if(!aggregatorInfo.isAggregator(aggregatorName)) {
          logger.error(aggregatorName + " is not a valid aggregator.");
          return false;
        }

        if(aggregatorInfo.isStaticAggregator(aggregatorName)) {
          aggregatorNames.add(aggregatorName);
          continue;
        }

        aggregatorNames.addAll(aggregatorInfo.getOTFAggregatorToStaticAggregators().get(aggregatorName));
      }

      for(String aggregatorName: aggregatorNames) {
        Integer aggregatorID = AggregatorStaticType.NAME_TO_ORDINAL.get(aggregatorName);

        EventKey eventKey = new EventKey(SCHEMA_ID,
                                         ddID,
                                         aggregatorID,
                                         gpoKey);
        aggregatorToEventKey.put(aggregatorName, eventKey);
      }

      long bucketKey = getBucketForSchema(SCHEMA_ID);

      List<Map<String, EventKey>> eventKeys = Lists.newArrayList();
      List<Map<String, HDSQuery>> hdsQueries = Lists.newArrayList();

      if(!query.isHasTime()) {
        Map<String, HDSQuery> aggregatorToQueryMap = Maps.newHashMap();
        Map<String, EventKey> aggregatorToEventKeyMap = Maps.newHashMap();

        for(Map.Entry<String, EventKey> entry: aggregatorToEventKey.entrySet()) {
          String aggregatorName = entry.getKey();
          EventKey eventKey = entry.getValue();
          Slice key = new Slice(getEventKeyBytesGAE(eventKey));

          HDSQuery hdsQuery = operator.queries.get(key);

          if(hdsQuery == null) {
            hdsQuery = new HDSQuery();
            hdsQuery.bucketKey = bucketKey;
            hdsQuery.key = key;
            operator.addQuery(hdsQuery);
          }
          else {
            if(hdsQuery.result == null) {
              logger.debug("Forcing refresh for {}", hdsQuery);
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

        for(long timestamp = startTime;
            timestamp <= endTime;
            timestamp += query.getTimeBucket().getTimeUnit().toMillis(1)) {

          Map<String, HDSQuery> aggregatorToQueryMap = Maps.newHashMap();
          Map<String, EventKey> aggregatorToEventKeyMap = Maps.newHashMap();

          for(Map.Entry<String, EventKey> entry: aggregatorToEventKey.entrySet()) {
            String aggregatorName = entry.getKey();
            EventKey eventKey = entry.getValue();

            gpoKey.setField(DimensionsDescriptor.DIMENSION_TIME, timestamp);
            gpoKey.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, query.getTimeBucket().ordinal());

            EventKey queryEventKey = new EventKey(eventKey);
            Slice key = new Slice(getEventKeyBytesGAE(eventKey));

            HDSQuery hdsQuery = operator.queries.get(key);

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
  }

  class DimensionsQueryComputer implements QueryComputer<DataQueryDimensional, QueryMeta, MutableLong, MutableBoolean, Result>
  {
    private final AppDataSingleSchemaDimensionStoreHDHT operator;

    public DimensionsQueryComputer(AppDataSingleSchemaDimensionStoreHDHT operator)
    {
      this.operator = operator;
    }

    @Override
    public Result processQuery(DataQueryDimensional query, QueryMeta qm, MutableLong queueContext, MutableBoolean context)
    {
      logger.debug("Processing query {} with countdown {}", query.getId(), query.getCountdown());

      List<Map<String, GPOMutable>> keys = Lists.newArrayList();
      List<Map<String, GPOMutable>> values = Lists.newArrayList();

      List<Map<String, HDSQuery>> queries = qm.getHdsQueries();
      List<Map<String, EventKey>> eventKeys = qm.getEventKeys();

      boolean allSatisfied = true;

      for(int index = 0; index < queries.size(); index++) {
        Map<String, HDSQuery> aggregatorToQuery = queries.get(index);
        Map<String, EventKey> aggregatorToEventKey = eventKeys.get(index);

        Map<String, GPOMutable> aggregatorKeys = Maps.newHashMap();
        Map<String, GPOMutable> aggregatorValues = Maps.newHashMap();

        for(String aggregatorName: aggregatorToQuery.keySet()) {
          HDSQuery hdsQuery = aggregatorToQuery.get(aggregatorName);

          EventKey eventKey = aggregatorToEventKey.get(aggregatorName);

          AggregateEvent gae;
          gae = operator.cache.getIfPresent(eventKey);

          if(gae != null) {
            logger.debug("Retrieved from cache.");

            if(gae.getKeys() == null) {
              logger.debug("A Keys are null and they shouldn't be");
            }

            aggregatorKeys.put(aggregatorName, gae.getKeys());
            aggregatorValues.put(aggregatorName, gae.getAggregates());
          }
          else {
            Slice keySlice = new Slice(operator.getEventKeyBytesGAE(eventKey));
            byte[] value = operator.getUncommitted(AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID,
                                                   keySlice);

            if(value != null) {
              gae = operator.fromKeyValueGAE(keySlice, value);
              aggregatorKeys.put(aggregatorName, gae.getKeys());
              aggregatorValues.put(aggregatorName, gae.getAggregates());
              logger.debug("Retrieved from uncommited");
            }
            else if(hdsQuery.result != null) {
              gae = operator.codec.fromKeyValue(hdsQuery.key, hdsQuery.result);

              if(gae.getKeys() == null) {
                logger.debug("B Keys are null and they shouldn't be");
              }

              logger.debug("Retrieved from hds");
              aggregatorKeys.put(aggregatorName, gae.getKeys());
              aggregatorValues.put(aggregatorName, gae.getAggregates());
            }
            else {
              allSatisfied = false;
            }

            if(hdsQuery.processed) {
              hdsQuery.processed = false;
            }
          }
        }

        if(!aggregatorValues.isEmpty()) {
          keys.add(aggregatorKeys);
          values.add(aggregatorValues);
        }
      }

      if(!query.getIncompleteResultOK() &&
         !allSatisfied && queueContext.longValue() > 1L) {
        return null;
      }

      List<Map<String, GPOMutable>> prunedKeys = Lists.newArrayList();
      List<Map<String, GPOMutable>> prunedValues = Lists.newArrayList();

      for(int index = 0;
          index < keys.size();
          index++) {
        Map<String, GPOMutable> key = keys.get(index);
        Map<String, GPOMutable> value = values.get(index);

        Map<String, GPOMutable> prunedKey = Maps.newHashMap();
        Map<String, GPOMutable> prunedValue = Maps.newHashMap();

        if(key.isEmpty()) {
          continue;
        }

        GPOMutable singleKey = key.entrySet().iterator().next().getValue();
        boolean completeTimeBucket = true;

        for(String aggregatorName: query.getFieldsAggregatable().getAggregators())
        {
          if(aggregatorInfo.isStaticAggregator(aggregatorName)) {
            GPOMutable valueGPO = value.get(aggregatorName);

            if(valueGPO == null) {
              completeTimeBucket = false;
              break;
            }

            prunedKey.put(aggregatorName, key.get(aggregatorName));
            prunedValue.put(aggregatorName, value.get(aggregatorName));

            continue;
          }

          List<GPOMutable> mutableValues = Lists.newArrayList();
          List<String> childAggregators = aggregatorInfo.getOTFAggregatorToStaticAggregators().get(aggregatorName);

          boolean gotAllStaticAggregators = true;

          for(String childAggregator: childAggregators) {
            GPOMutable valueGPO = value.get(childAggregator);

            if(valueGPO == null) {
              gotAllStaticAggregators = false;
              break;
            }

            mutableValues.add(valueGPO);
          }

          if(!gotAllStaticAggregators) {
            continue;
          }

          Set<String> fields = query.getFieldsAggregatable().getAggregatorToFields().get(aggregatorName);
          FieldsDescriptor fd =
          dimensionalSchema.getGenericEventSchema().getInputValuesDescriptor().getSubset(new Fields(fields));

          DimensionsOTFAggregator aggregator = AggregatorOTFType.NAME_TO_AGGREGATOR.get(aggregatorName);
          GPOMutable result = aggregator.aggregate(fd, mutableValues.toArray(new GPOMutable[mutableValues.size()]));
          prunedValue.put(aggregatorName, result);
          prunedKey.put(aggregatorName, singleKey);
        }

        if(completeTimeBucket) {
          prunedKeys.add(prunedKey);
          prunedValues.add(prunedValue);
        }
      }

      return new DataResultDimensional(query,
                            prunedKeys,
                            prunedValues,
                            queueContext.longValue());
    }

    @Override
    public void queueDepleted(MutableBoolean context)
    {
      context.setValue(true);
    }
  }

  static class QueryMeta
  {
    private List<Map<String, HDSQuery>> hdsQueries;
    private List<Map<String, EventKey>> eventKeys;

    public QueryMeta()
    {
    }

    /**
     * @return the hdsQueries
     */
    public List<Map<String, HDSQuery>> getHdsQueries()
    {
      return hdsQueries;
    }

    /**
     * @param hdsQueries the hdsQueries to set
     */
    public void setHdsQueries(List<Map<String, HDSQuery>> hdsQueries)
    {
      this.hdsQueries = hdsQueries;
    }

    /**
     * @return the event keys
     */
    public List<Map<String, EventKey>> getEventKeys()
    {
      return eventKeys;
    }

    /**
     * @param eventKeys event keys to set
     */
    public void setEventKeys(List<Map<String, EventKey>> eventKeys)
    {
      this.eventKeys = eventKeys;
    }
  }
}
