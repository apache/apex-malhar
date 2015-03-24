/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.AppDataQueryPort;
import com.datatorrent.api.annotation.AppDataResultPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.Slice;


import com.datatorrent.contrib.dimensions.GenericDimensionsStoreHDHT;
import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.demos.dimensions.ads.AggType;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent.EventKey;
import com.datatorrent.lib.appdata.dimensions.GenericEventSchema;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import com.datatorrent.lib.appdata.qr.DataSerializerFactory;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.processor.AppDataWWEQueryQueueManager;
import com.datatorrent.lib.appdata.qr.processor.QueryComputer;
import com.datatorrent.lib.appdata.qr.processor.QueryProcessor;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.GenericDataQuery;
import com.datatorrent.lib.appdata.schemas.GenericDataResult;
import com.datatorrent.lib.appdata.schemas.GenericSchemaDimensional;
import com.datatorrent.lib.appdata.schemas.GenericSchemaResult;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.google.common.collect.Lists;
import java.io.Serializable;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericAdsDimensionStore extends GenericDimensionsStoreHDHT implements Serializable
{
  private static final long serialVersionUID = 201503231218L;
  private static final Logger logger = LoggerFactory.getLogger(GenericAdsDimensionStore.class);

  public static final int SCHEMA_ID = 0;

  @NotNull
  private String eventSchemaJSON;
  @NotNull
  private String dimensionalSchemaJSON;

  private transient GenericEventSchema eventSchema;
  private transient GenericSchemaDimensional dimensionalSchema;
  private transient List<List<FieldsDescriptor>> indexToFieldsDescriptor;


  //==========================================================================
  // Query Processing - Start
  //==========================================================================

  private transient QueryProcessor<GenericDataQuery, AdsQueryMeta, MutableLong, MutableBoolean, Result> queryProcessor;
  @SuppressWarnings("unchecked")
  private transient DataDeserializerFactory queryDeserializerFactory;
  private transient DataSerializerFactory resultSerializerFactory;
  private static final Long QUERY_QUEUE_WINDOW_COUNT = 30L;
  private static final int QUERY_QUEUE_WINDOW_COUNT_INT = (int) ((long) QUERY_QUEUE_WINDOW_COUNT);

  private transient long windowId;

    @AppDataResultPort(schemaType = "default", schemaVersion = "1.0")
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<String>();

  @InputPortFieldAnnotation(optional = true)
  @AppDataQueryPort
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
        String schemaResult =
        resultSerializerFactory.serialize(new GenericSchemaResult((SchemaQuery)query,
                                                                  dimensionalSchema));
        queryResult.emit(schemaResult);
      }
      else if(query instanceof GenericDataQuery) {
        GenericDataQuery gdq = (GenericDataQuery) query;
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

  public GenericAdsDimensionStore()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    eventSchema = new GenericEventSchema(eventSchemaJSON);
    dimensionalSchema = new GenericSchemaDimensional(dimensionalSchemaJSON);
    indexToFieldsDescriptor = eventSchema.getDdIDToAggregatorIDToFieldsDescriptor(AggType.NAME_TO_ORDINAL);
    super.setup(context);


    //Setup for query processing
    queryProcessor =
    new QueryProcessor<GenericDataQuery, AdsQueryMeta, MutableLong, MutableBoolean, Result>(
                                                  new AdsQueryComputer(this),
                                                  new AdsQueryQueueManager(this, QUERY_QUEUE_WINDOW_COUNT_INT));
    queryDeserializerFactory = new DataDeserializerFactory(SchemaQuery.class,
                                                           GenericDataQuery.class);
    queryDeserializerFactory.setContext(GenericDataQuery.class, dimensionalSchema);
    resultSerializerFactory = new DataSerializerFactory();

    queryProcessor.setup(context);
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
  public DimensionsAggregator<GenericAggregateEvent> getAggregator(int aggregatorID)
  {
    return AggType.values()[aggregatorID].getAggregator();
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
  public int getPartitionGAE(GenericAggregateEvent inputEvent)
  {
    return inputEvent.getEventKey().hashCode();
  }

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


  //==========================================================================
  // Query Processing Classes - Start
  //==========================================================================

  class AdsQueryQueueManager extends AppDataWWEQueryQueueManager<GenericDataQuery, AdsQueryMeta>
  {
    private GenericAdsDimensionStore operator;
    private int queueWindowCount;

    public AdsQueryQueueManager(GenericAdsDimensionStore operator,
                                int queueWindowCount)
    {
      this.operator = operator;
      this.queueWindowCount = queueWindowCount;
    }

    @Override
    public boolean enqueue(GenericDataQuery query, AdsQueryMeta queryMeta, MutableLong windowExpireCount)
    {
      logger.info("Enqueueing query {}", query);

      long endTime = -1L;
      long startTime = -1L;

      if(query.isFromTo()) {
        startTime = query.getFromLong();
        endTime = query.getToLong();

        if(query.getTimeBucket() == TimeBucket.MINUTE) {
          startTime = AdInfo.roundMinute(startTime);
          endTime = AdInfo.roundMinute(endTime);
        }
        else if(query.getTimeBucket() == TimeBucket.HOUR) {
          startTime = AdInfo.roundHour(startTime);
          endTime = AdInfo.roundHour(endTime);
        }
        else if(query.getTimeBucket() == TimeBucket.DAY) {
          startTime = AdInfo.roundDay(startTime);
          endTime = AdInfo.roundDay(endTime);
        }
      }
      else {
        long time = System.currentTimeMillis();

        if(query.getTimeBucket() == TimeBucket.MINUTE) {
          endTime = AdInfo.roundMinute(time);
        }
        else if(query.getTimeBucket() == TimeBucket.HOUR) {
          endTime = AdInfo.roundHour(time);
        }
        else if(query.getTimeBucket() == TimeBucket.DAY) {
          endTime = AdInfo.roundDay(time);
        }

        startTime = endTime - query.getTimeBucket().getTimeUnit().toMillis(query.getLatestNumBuckets());
      }

      Integer ddID = eventSchema.getFieldsToDimensionDescriptor().get(query.getKeyFields());

      if(ddID == null) {
        logger.error("No aggregations for keys: {}", query.getKeyFields());
        return false;
      }

      FieldsDescriptor dd = eventSchema.getDdIDToKeyDescriptor().get(ddID);
      GPOMutable gpoKey = query.createKeyGPO(dd);

      EventKey eventKey = new EventKey(SCHEMA_ID,
                                       ddID,
                                       AggType.SUM.ordinal(),
                                       gpoKey);

      long bucketKey = getBucketForSchema(SCHEMA_ID);

      List<HDSQuery> hdsQueries = Lists.newArrayList();

      for(long timestamp = startTime;
          timestamp <= endTime;
          timestamp += query.getTimeBucket().getTimeUnit().toMillis(1)) {
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

        hdsQuery.keepAliveCount = (int) query.getCountdown();
        hdsQueries.add(hdsQuery);
      }

      AdsQueryMeta aqm = new AdsQueryMeta();
      aqm.setBeginTime(startTime);
      aqm.setEventKey(eventKey);
      aqm.setHdsQueries(hdsQueries);

      return super.enqueue(query, aqm, new MutableLong(query.getCountdown()));
    }
  }

  class AdsQueryComputer implements QueryComputer<GenericDataQuery, AdsQueryMeta, MutableLong, MutableBoolean, Result>
  {
    private GenericAdsDimensionStore operator;

    public AdsQueryComputer(GenericAdsDimensionStore operator)
    {
      this.operator = operator;
    }

    @Override
    public Result processQuery(GenericDataQuery query, AdsQueryMeta adsQueryMeta, MutableLong queueContext, MutableBoolean context)
    {
      logger.debug("Processing query {}", query);

      List<GPOMutable> values = Lists.newArrayList();

      GenericDataResult dataResult =
      new GenericDataResult(query,
                            values,
                            queueContext.longValue());

      EventKey eventKey = adsQueryMeta.getEventKey();
      TimeUnit bucketUnit = query.getTimeBucket().getTimeUnit();
      Iterator<HDSQuery> queryIt = adsQueryMeta.getHdsQueries().iterator();

      boolean allSatisfied = true;

      for(long timestamp = adsQueryMeta.getBeginTime();
          queryIt.hasNext();
          timestamp += bucketUnit.toMillis(1))
      {
        HDSQuery hdsQuery = queryIt.next();
        eventKey.getKey().setField(DimensionsDescriptor.DIMENSION_TIME, timestamp);

        GenericAggregateEvent gae = nonWaitingCache.get(eventKey);

        // TODO
        // There is a race condition with retrieving from the cache and doing
        // an hds query. If an hds query finishes for a key while it is in the minuteCache, but
        // then that key gets evicted from the minuteCache, then the value will never be retrieved.
        // A list of evicted keys should be kept, so that corresponding queries can be refreshed.
        if(gae != null) {
          logger.debug("Adding from aggregation buffer");
          values.add(gae.getAggregates());
        }
        else if(hdsQuery.processed &&
                hdsQuery.result != null) {
          GenericAggregateEvent tgae = operator.codec.fromKeyValue(hdsQuery.key, hdsQuery.result);
          values.add(tgae.getAggregates());
        }
        else {
          allSatisfied = false;
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

      return dataResult;
    }

    @Override
    public void queueDepleted(MutableBoolean context)
    {
      context.setValue(true);
    }
  }

  static class AdsQueryMeta
  {
    private long beginTime;
    private List<HDSQuery> hdsQueries;
    private EventKey eventKey;

    public AdsQueryMeta()
    {
    }

    /**
     * @return the hdsQueries
     */
    public List<HDSQuery> getHdsQueries()
    {
      return hdsQueries;
    }

    /**
     * @param hdsQueries the hdsQueries to set
     */
    public void setHdsQueries(List<HDSQuery> hdsQueries)
    {
      this.hdsQueries = hdsQueries;
    }

    /**
     * @return the adInofAggregateEvent
     */
    public EventKey getEventKey()
    {
      return eventKey;
    }

    /**
     * @param adInofAggregateEvent the adInofAggregateEvent to set
     */
    public void setEventKey(EventKey eventKey)
    {
      this.eventKey = eventKey;
    }

    /**
     * @return the beginTime
     */
    public long getBeginTime()
    {
      return beginTime;
    }

    /**
     * @param beginTime the beginTime to set
     */
    public void setBeginTime(long beginTime)
    {
      this.beginTime = beginTime;
    }
  }
}
