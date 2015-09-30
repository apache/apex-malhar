/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.dimensions.sales.generic;

import java.util.Map;

import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaMap;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.demos.dimensions.InputGenerator;

/**
 * @since 3.1.0
 */

@ApplicationAnnotation(name=SalesDemo.APP_NAME)
public class SalesDemo implements StreamingApplication
{
  public static final String APP_NAME = "SalesDemo";
  public static final String PROP_STORE_PATH = "dt.application." + APP_NAME + ".operator.Store.fileStore.basePathPrefix";

  public static final String EVENT_SCHEMA = "salesGenericEventSchema.json";
  public static final String DIMENSIONAL_SCHEMA = "salesGenericDataSchema.json";

  public InputGenerator<byte[]> inputGenerator;

  public SalesDemo()
  {
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);

    if(inputGenerator == null) {
      JsonSalesGenerator input = dag.addOperator("InputGenerator", JsonSalesGenerator.class);
      input.setEventSchemaJSON(eventSchema);
      inputGenerator = input;
    }
    else {
      dag.addOperator("InputGenerator", inputGenerator);
    }

    JsonToMapConverter converter = dag.addOperator("Converter", JsonToMapConverter.class);
    EnrichmentOperator enrichmentOperator = dag.addOperator("Enrichment", EnrichmentOperator.class);
    DimensionsComputationFlexibleSingleSchemaMap dimensions =
    dag.addOperator("DimensionsComputation", DimensionsComputationFlexibleSingleSchemaMap.class);
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);

    String basePath = conf.get(PROP_STORE_PATH);
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    basePath += System.currentTimeMillis();
    hdsFile.setBasePath(basePath);

    store.setFileStore(hdsFile);
    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator< MutableLong >());
    String dimensionalSchema = SchemaUtils.jarResourceFileToString(DIMENSIONAL_SCHEMA);

    dimensions.setConfigurationSchemaJSON(eventSchema);
    Map<String, String> fieldToMapField = Maps.newHashMap();
    dimensions.setValueNameAliases(fieldToMapField);
    dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());
    dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB, 8092);

    store.setConfigurationSchemaJSON(eventSchema);
    store.setDimensionalSchemaStubJSON(dimensionalSchema);

    PubSubWebSocketAppDataQuery wsIn = new PubSubWebSocketAppDataQuery();
    store.setEmbeddableQueryInfoProvider(wsIn);

    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());

    dag.addStream("InputStream", inputGenerator.getOutputPort(), converter.input);
    dag.addStream("EnrichmentStream", converter.outputMap, enrichmentOperator.inputPort);
    dag.addStream("ConvertStream", enrichmentOperator.outputPort, dimensions.input);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("QueryResult", store.queryResult, wsOut.input).setLocality(Locality.CONTAINER_LOCAL);
  }

  private static final Logger LOG = LoggerFactory.getLogger(SalesDemo.class);
}
