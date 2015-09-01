/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.ads.stats;

import java.net.URI;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputation;
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

import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregator;
import com.datatorrent.demos.dimensions.ads.InputItemGenerator;

/**
 * @since 3.1.0
 */

@ApplicationAnnotation(name=AdsDimensionsDemoPerformant.APP_NAME)
public class AdsDimensionsDemoPerformant implements StreamingApplication
{
  public static final String EVENT_SCHEMA = "adsBenchmarkSchema.json";
  public static final String APP_NAME = "AdsDimensionsDemoFast";
  public static final String PROP_STORE_PATH = "dt.application." + APP_NAME + ".operator.Store.fileStore.basePathPrefix";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //Declare operators

    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
    DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent>());
    DimensionsComputationUnifierImpl<AdInfo, AdInfo.AdInfoAggregateEvent> unifier = new DimensionsComputationUnifierImpl<AdInfo, AdInfo.AdInfoAggregateEvent>();
    dimensions.setUnifier(unifier);

    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 10);
    AdsConverter adsConverter = dag.addOperator("AdsConverter", new AdsConverter());
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);

    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);
    input.setEventSchemaJSON(eventSchema);

    String[] dimensionSpecs = new String[] {
      "time=" + TimeUnit.MINUTES,
      "time=" + TimeUnit.MINUTES + ":location",
      "time=" + TimeUnit.MINUTES + ":advertiser",
      "time=" + TimeUnit.MINUTES + ":publisher",
      "time=" + TimeUnit.MINUTES + ":advertiser:location",
      "time=" + TimeUnit.MINUTES + ":publisher:location",
      "time=" + TimeUnit.MINUTES + ":publisher:advertiser",
      "time=" + TimeUnit.MINUTES + ":publisher:advertiser:location"
    };

    //Set operator properties
    AdInfoAggregator[] aggregators = new AdInfoAggregator[dimensionSpecs.length];

    //Set input properties
    input.setEventSchemaJSON(eventSchema);

    for(int index = 0;
        index < dimensionSpecs.length;
        index++) {
      String dimensionSpec = dimensionSpecs[index];
      AdInfoAggregator aggregator = new AdInfoAggregator();
      aggregator.init(dimensionSpec, index);
      aggregators[index] = aggregator;
    }

    unifier.setAggregators(aggregators);
    dimensions.setAggregators(aggregators);
    dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB, 8092);

    //Configuring the converter
    adsConverter.setEventSchemaJSON(eventSchema);
    adsConverter.setDimensionSpecs(dimensionSpecs);

    //Set store properties
    String basePath = Preconditions.checkNotNull(conf.get(PROP_STORE_PATH),
                                                 "a base path should be specified in the properties.xml");
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    basePath += Path.SEPARATOR + System.currentTimeMillis();
    hdsFile.setBasePath(basePath);
    store.setFileStore(hdsFile);
    store.getResultFormatter().setContinuousFormatString("#.00");
    store.setConfigurationSchemaJSON(eventSchema);

    PubSubWebSocketAppDataQuery wsIn = new PubSubWebSocketAppDataQuery();
    store.setEmbeddableQueryInfoProvider(wsIn);

    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());

    //Set remaining dag options

    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    dag.addStream("InputStream", input.outputPort, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, adsConverter.inputPort);
    dag.addStream("Converter", adsConverter.outputPort, store.input);
    dag.addStream("QueryResult", store.queryResult, wsOut.input);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AdsDimensionsDemoPerformant.class);
}
