/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.ads.generic;

import java.net.URI;

import java.util.List;
import java.util.Map;


import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
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
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.demos.dimensions.InputGenerator;
import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.demos.dimensions.ads.InputItemGenerator;


/**
 * An AdsDimensionsDemo run with HDHT
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name=AdsDimensionsDemo.APP_NAME)
public class AdsDimensionsDemo implements StreamingApplication
{
  public static final String APP_NAME = "AdsDimensionsDemoGeneric";
  public static final String EVENT_SCHEMA = "adsGenericEventSchema.json";

  public String appName = APP_NAME;
  public String eventSchemaLocation = EVENT_SCHEMA;
  public List<Object> advertisers;
  public InputGenerator<AdInfo> inputOperator;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String propStorePath = "dt.application." + appName + ".operator.Store.fileStore.basePathPrefix";

    //Declare operators

    //Set input properties
    String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);

    if(inputOperator == null) {
      InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
      input.advertiserName = advertisers;
      input.setEventSchemaJSON(eventSchema);
      inputOperator = input;
    }
    else {
      dag.addOperator("InputGenerator", inputOperator);
    }

    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("DimensionsComputation", DimensionsComputationFlexibleSingleSchemaPOJO.class);
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 4);
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);

    //Set operator properties

    Map<String, String> keyToExpression = Maps.newHashMap();
    keyToExpression.put("publisher", "getPublisher()");
    keyToExpression.put("advertiser", "getAdvertiser()");
    keyToExpression.put("location", "getLocation()");
    keyToExpression.put("time", "getTime()");

    Map<String, String> aggregateToExpression = Maps.newHashMap();
    aggregateToExpression.put("cost", "getCost()");
    aggregateToExpression.put("revenue", "getRevenue()");
    aggregateToExpression.put("impressions", "getImpressions()");
    aggregateToExpression.put("clicks", "getClicks()");

    dimensions.setKeyToExpression(keyToExpression);
    dimensions.setAggregateToExpression(aggregateToExpression);
    dimensions.setConfigurationSchemaJSON(eventSchema);

    dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());
    dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB, 8092);

    //Set store properties
    String basePath = Preconditions.checkNotNull(conf.get(propStorePath),
                                                 "a base path should be specified in the properties.xml");
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    basePath += Path.SEPARATOR + System.currentTimeMillis();
    hdsFile.setBasePath(basePath);
    System.out.println("Setting basePath " + basePath);
    store.setFileStore(hdsFile);
    store.getResultFormatter().setContinuousFormatString("#.00");
    store.setConfigurationSchemaJSON(eventSchema);

    store.setEmbeddableQueryInfoProvider(new PubSubWebSocketAppDataQuery());
    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());

    //Set remaining dag options

    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    dag.addStream("InputStream", inputOperator.getOutputPort(), dimensions.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("QueryResult", store.queryResult, wsOut.input);
  }
}

