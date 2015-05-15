/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import com.datatorrent.api.AppData;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.appdata.dimensions.AggregatorInfo;
import com.datatorrent.lib.appdata.dimensions.AggregatorUtils;
import com.datatorrent.lib.appdata.dimensions.DimensionsStaticAggregator;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import com.datatorrent.lib.appdata.qr.DataSerializerFactory;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.processor.QueryProcessor;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAppDataDimensionStoreHDHT extends DimensionsStoreHDHT
{
  @NotNull
  protected ResultFormatter appDataFormatter = new ResultFormatter();
  @NotNull
  protected AggregatorInfo aggregatorInfo = AggregatorUtils.DEFAULT_AGGREGATOR_INFO;

  //Query Processing - Start
  protected transient QueryProcessor<DataQueryDimensional, QueryMeta, MutableLong, MutableBoolean, Result> queryProcessor;
  protected final transient DataDeserializerFactory queryDeserializerFactory;

  @VisibleForTesting
  public SchemaRegistry schemaRegistry;
  protected transient DataSerializerFactory resultSerializerFactory;

  @AppData.ResultPort
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<String>();

  @InputPortFieldAnnotation(optional = true)
  @AppData.QueryPort
  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      LOG.debug("Received {}", s);
      Data query;
      try {
        query = queryDeserializerFactory.deserialize(s);
      }
      catch (IOException ex) {
        LOG.error("error parsing query {}", s, ex);
        return;
      }

      if (query instanceof SchemaQuery) {
        processSchemaQuery((SchemaQuery) query);
      }
      else if (query instanceof DataQueryDimensional) {
        processDimensionalDataQuery((DataQueryDimensional) query);
      }
      else {
        LOG.error("Invalid query {}", s);
      }
    }
  };

  @SuppressWarnings("unchecked")
  public AbstractAppDataDimensionStoreHDHT()
  {
    queryDeserializerFactory = new DataDeserializerFactory(SchemaQuery.class, DataQueryDimensional.class);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    aggregatorInfo.setup();

    schemaRegistry = getSchemaRegistry();

    //setup query processor
    queryProcessor = QueryProcessor.newInstance(new DimensionsQueryComputer(this, schemaRegistry),
      new DimensionsQueryQueueManager(this, schemaRegistry));
    queryProcessor.setup(context);

    resultSerializerFactory = new DataSerializerFactory(appDataFormatter);
    queryDeserializerFactory.setContext(DataQueryDimensional.class, schemaRegistry);
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    queryProcessor.beginWindow(windowId);
    super.beginWindow(windowId);
  }

  protected void processDimensionalDataQuery(DataQueryDimensional dataQueryDimensional)
  {
    queryProcessor.enqueue(dataQueryDimensional, null, null);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();

    MutableBoolean done = new MutableBoolean(false);

    while (done.isFalse()) {
      Result aotr = queryProcessor.process(done);

      if (aotr != null) {
        String result = resultSerializerFactory.serialize(aotr);
        LOG.debug("Emitting the result: {}", result);
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

  /**
   * Processes schema queries
   *
   * @param schemaQuery a schema query
   */
  protected abstract void processSchemaQuery(SchemaQuery schemaQuery);

  /**
   * @return the schema registry
   */
  protected abstract SchemaRegistry getSchemaRegistry();

  @Override
  public DimensionsStaticAggregator getAggregator(int aggregatorID)
  {
    return aggregatorInfo.getStaticAggregatorIDToAggregator().get(aggregatorID);
  }

  @Override
  protected int getAggregatorID(String aggregatorName)
  {
    return aggregatorInfo.getStaticAggregatorNameToID().get(aggregatorName);
  }

  public void setAppDataFormatter(ResultFormatter appDataFormatter)
  {
    this.appDataFormatter = appDataFormatter;
  }

  /**
   * @return the appDataFormatter
   */
  public ResultFormatter getAppDataFormatter()
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

  private static final Logger LOG = LoggerFactory.getLogger(AbstractAppDataDimensionStoreHDHT.class);
}
