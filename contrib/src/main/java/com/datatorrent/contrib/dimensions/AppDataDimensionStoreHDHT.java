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

import com.datatorrent.api.AppData;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent.EventKey;
import com.datatorrent.lib.appdata.dimensions.AggregatorType;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import com.datatorrent.lib.appdata.qr.DataSerializerFactory;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.processor.AppDataWWEQueryQueueManager;
import com.datatorrent.lib.appdata.qr.processor.QueryComputer;
import com.datatorrent.lib.appdata.qr.processor.QueryProcessor;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DataResultDimensional;
import com.datatorrent.lib.appdata.schemas.DimensionalEventSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaDimensional;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Serializable;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class AppDataDimensionStoreHDHT extends DimensionsStoreHDHT implements Serializable
{
  private static final long serialVersionUID = 201503231218L;
  private static final Logger logger = LoggerFactory.getLogger(AppDataDimensionStoreHDHT.class);

  public static final int SCHEMA_ID = 0;

  @NotNull
  private String eventSchemaJSON;
  @NotNull
  private String dimensionalSchemaJSON;

  private transient DimensionalEventSchema eventSchema;
  private transient SchemaDimensional dimensionalSchema;
  private transient List<Map<Integer, FieldsDescriptor>> indexToFieldsDescriptor;

  //==========================================================================
  // Query Processing - Start
  //==========================================================================

  private transient QueryProcessor<DataQueryDimensional, QueryMeta, MutableLong, MutableBoolean, Result> queryProcessor;
  @SuppressWarnings("unchecked")
  private transient DataDeserializerFactory queryDeserializerFactory;
  private transient DataSerializerFactory resultSerializerFactory;
  private static final Long QUERY_QUEUE_WINDOW_COUNT = 30L;
  private static final int QUERY_QUEUE_WINDOW_COUNT_INT = (int) ((long) QUERY_QUEUE_WINDOW_COUNT);

  private transient long windowId;

  @AppData.ResultPort(schemaType = "default", schemaVersion = "1.0")
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<String>();

  @InputPortFieldAnnotation(optional = true)
  @AppData.QueryPort
  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override public void process(String s)
    {
      logger.info("Received: {}", s);

      Data query = queryDeserializerFactory.deserialize(s);

      //Query was not parseable
      if(query == null) {
        logger.info("Not parseable.");
        return;
      }

      if(query instanceof SchemaQuery) {
        try{
        String schemaResult =
        resultSerializerFactory.serialize(new SchemaResult((SchemaQuery)query,
                                                           dimensionalSchema));
        queryResult.emit(schemaResult);}
        catch(Exception e) {
          logger.error("Exception {}", e);
        }
      }
      else if(query instanceof DataQueryDimensional) {
        DataQueryDimensional gdq = (DataQueryDimensional) query;
        logger.info("GDQ: {}", gdq);
        queryProcessor.enqueue(gdq, null, null);
      }
      else {
        logger.error("Invalid query {}", s);
      }
    }
  };

  //==========================================================================
  // Query Processing - End
  //==========================================================================

  public AppDataDimensionStoreHDHT()
  {
  }

  @Override
  public void processEvent(AggregateEvent gae) {
    super.processEvent(gae);
  }

  @Override
  public void setup(OperatorContext context)
  {
    //Setup for query processing
    queryProcessor =
    new QueryProcessor<DataQueryDimensional, QueryMeta, MutableLong, MutableBoolean, Result>(
                                                  new DimensionsQueryComputer(this),
                                                  new DimensionsQueryQueueManager(this, QUERY_QUEUE_WINDOW_COUNT_INT));
    queryDeserializerFactory = new DataDeserializerFactory(SchemaQuery.class,
                                                           DataQueryDimensional.class);
    eventSchema = new DimensionalEventSchema(eventSchemaJSON);
    dimensionalSchema = new SchemaDimensional(eventSchemaJSON,
                                              AggregatorType.NAME_TO_AGGREGATOR);
    queryDeserializerFactory.setContext(DataQueryDimensional.class, dimensionalSchema);
    indexToFieldsDescriptor = eventSchema.getDdIDToAggregatorIDToFieldsDescriptor(AggregatorType.NAME_TO_ORDINAL);
    resultSerializerFactory = new DataSerializerFactory();
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
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

      if(done.isFalse()) {
        logger.debug("Query: {}", this.windowId);
      }

      if(aotr != null) {
        String result = resultSerializerFactory.serialize(aotr);
        logger.info("Emitting the result: {}", result);
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
  public DimensionsAggregator getAggregator(int aggregatorID)
  {
    return AggregatorType.values()[aggregatorID].getAggregator();
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

    return indexToFieldsDescriptor.get(dimensionsDescriptorID).get(aggregatorID);
  }

  @Override
  public long getBucketForSchema(int schemaID)
  {
    return 0;
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
    return AggregatorType.NAME_TO_ORDINAL.get(aggregatorName);
  }

  //==========================================================================
  // Query Processing Classes - Start
  //==========================================================================

  class DimensionsQueryQueueManager extends AppDataWWEQueryQueueManager<DataQueryDimensional, QueryMeta>
  {
    private AppDataDimensionStoreHDHT operator;
    private int queueWindowCount;

    public DimensionsQueryQueueManager(AppDataDimensionStoreHDHT operator,
                                int queueWindowCount)
    {
      this.operator = operator;
      this.queueWindowCount = queueWindowCount;
    }

    @Override
    public boolean enqueue(DataQueryDimensional query, QueryMeta queryMeta, MutableLong windowExpireCount)
    {
      Integer ddID = eventSchema.getDimensionsDescriptorToID().get(query.getDd());

      if(ddID == null) {
        logger.error("No aggregations for keys: {}", query.getKeyFields());
        return false;
      }

      FieldsDescriptor dd = eventSchema.getDdIDToKeyDescriptor().get(ddID);
      GPOMutable gpoKey = query.createKeyGPO(dd);

      Map<String, EventKey> aggregatorToEventKey = Maps.newHashMap();

      for(String aggregatorName: query.getFieldsAggregatable().getAggregators()) {
        EventKey eventKey = new EventKey(SCHEMA_ID,
                                         ddID,
                                         AggregatorType.SUM.ordinal(),
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

          hdsQuery.keepAliveCount = (int)query.getCountdown();
          aggregatorToEventKeyMap.put(aggregatorName, eventKey);
          aggregatorToQueryMap.put(aggregatorName, hdsQuery);
        }

        hdsQueries.add(aggregatorToQueryMap);
        eventKeys.add(aggregatorToEventKeyMap);
      }
      else {
        long endTime = -1L;
        long startTime = -1L;

        if(query.isFromTo()) {
          startTime = query.getTimeBucket().roundDown(query.getFromLong());
          endTime = query.getTimeBucket().roundDown(query.getToLong());
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

            hdsQuery.keepAliveCount = (int)query.getCountdown();

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
    private AppDataDimensionStoreHDHT operator;

    public DimensionsQueryComputer(AppDataDimensionStoreHDHT operator)
    {
      this.operator = operator;
    }

    @Override
    public Result processQuery(DataQueryDimensional query, QueryMeta qm, MutableLong queueContext, MutableBoolean context)
    {
      logger.info("Processing query {} with countdown {}", query.getId(), query.getCountdown());
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
          logger.info("Getting data with timestamp {}", eventKey.getKey().getField(DimensionsDescriptor.DIMENSION_TIME));

          // TODO
          // There is a race condition with retrieving from the cache and doing
          // an hds query. If an hds query finishes for a key while it is in the minuteCache, but
          // then that key gets evicted from the minuteCache, then the value will never be retrieved.
          // A list of evicted keys should be kept, so that corresponding queries can be refreshed.
          if(gae != null) {
            logger.info("Retrieved from cache.");
            aggregatorKeys.put(aggregatorName, gae.getKeys());
            aggregatorValues.put(aggregatorName, gae.getAggregates());
          }
          else {

            if(hdsQuery.processed) {
              if(hdsQuery.result != null) {
                AggregateEvent tgae = operator.codec.fromKeyValue(hdsQuery.key, hdsQuery.result);

                aggregatorKeys.put(aggregatorName, tgae.getKeys());
                aggregatorValues.put(aggregatorName, tgae.getAggregates());
              }
              else {
                allSatisfied = false;
              }

              hdsQuery.processed = false;
            }
            else {
              allSatisfied = false;
            }
          }
        }

        if(!aggregatorValues.isEmpty()) {
          keys.add(aggregatorKeys);
          values.add(aggregatorValues);
        }
      }

      if(!query.getIncompleteResultOK()) {
        if(!allSatisfied && queueContext.longValue() > 1L) {
          return null;
        }
        else {
          queueContext.setValue(0L);
        }
      }

      return new DataResultDimensional(query,
                            keys,
                            values,
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
     * @return the adInofAggregateEvent
     */
    public List<Map<String, EventKey>> getEventKeys()
    {
      return eventKeys;
    }

    /**
     * @param adInofAggregateEvent the adInofAggregateEvent to set
     */
    public void setEventKeys(List<Map<String, EventKey>> eventKeys)
    {
      this.eventKeys = eventKeys;
    }
  }
}
