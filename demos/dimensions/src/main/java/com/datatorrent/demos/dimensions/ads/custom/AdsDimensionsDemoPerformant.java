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

package com.datatorrent.demos.dimensions.ads.custom;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoSumAggregator;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdsDimensionsCombination;
import com.datatorrent.demos.dimensions.ads.InputItemGenerator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.DimensionsCombination;
import com.datatorrent.lib.dimensions.DimensionsComputationCustom;
import com.datatorrent.lib.dimensions.DimensionsComputationUnifierImpl;
import com.datatorrent.lib.dimensions.aggregator.Aggregator;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.net.URI;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


@ApplicationAnnotation(name=AdsDimensionsDemoPerformant.APP_NAME)
public class AdsDimensionsDemoPerformant implements StreamingApplication
{
  public static final String EVENT_SCHEMA = "adsBenchmarkSchema.json";
  public static final String APP_NAME = "AdsDimensionsDemoPerformant";
  public static final String PROP_STORE_PATH = "dt.application." + APP_NAME + ".operator.Store.fileStore.basePathPrefix";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //Declare operators

    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
    DimensionsComputationCustom<AdInfo, AdInfo.AdInfoAggregateEvent> dimensions = new TestDimensions();
    dag.addOperator("DimensionsComputation", dimensions);
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

    //Set input properties
    input.setEventSchemaJSON(eventSchema);

    //Set Dimensions properties
    LinkedHashMap<String, DimensionsCombination<AdInfo, AdInfo.AdInfoAggregateEvent>> dimensionsCombinations =
    Maps.newLinkedHashMap();

    LinkedHashMap<String, List<Aggregator<AdInfo, AdInfo.AdInfoAggregateEvent>>> dimensionsAggregators =
    Maps.newLinkedHashMap();

    for(int index = 0;
        index < dimensionSpecs.length;
        index++) {
      String dimensionSpec = dimensionSpecs[index];
      AdsDimensionsCombination dimensionsCombination = new AdsDimensionsCombination();
      dimensionsCombination.init(dimensionSpec, index);
      dimensionsCombinations.put(dimensionSpec, dimensionsCombination);

      List<Aggregator<AdInfo, AdInfo.AdInfoAggregateEvent>> aggregators = Lists.newArrayList();
      AdInfoSumAggregator adInfoSumAggregator = new AdInfoSumAggregator();
      aggregators.add(adInfoSumAggregator);
      dimensionsAggregators.put(dimensionSpec, aggregators);
    }

    DimensionsComputationUnifierImpl<AdInfo, AdInfo.AdInfoAggregateEvent> unifier = new DimensionsComputationUnifierImpl<AdInfo, AdInfo.AdInfoAggregateEvent>();
    dimensions.setUnifier(unifier);
    dimensions.setDimensionsCombinations(dimensionsCombinations);
    dimensions.setAggregators(dimensionsAggregators);

    //Configuring the converter
    adsConverter.setEventSchemaJSON(eventSchema);
    adsConverter.setDimensionSpecs(dimensionSpecs);

    //Set store properties
    String basePath = Preconditions.checkNotNull(conf.get(PROP_STORE_PATH),
                                                 "a base path should be specified in the properties.xml");
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    System.out.println(dag.getAttributes().get(DAG.APPLICATION_ID));
    basePath += Path.SEPARATOR + System.currentTimeMillis();
    hdsFile.setBasePath(basePath);
    System.out.println("Setting basePath " + basePath);
    store.setFileStore(hdsFile);
    store.getAppDataFormatter().setContinuousFormatString("#.00");
    store.setEventSchemaJSON(eventSchema);

    //Set pubsub properties
    Operator.OutputPort<String> queryPort;
    Operator.InputPort<String> queryResultPort;

    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
    //LOG.info("WebSocket with gateway at: {}", gatewayAddress);
    PubSubWebSocketAppDataQuery wsIn = dag.addOperator("Query", new PubSubWebSocketAppDataQuery());
    wsIn.setUri(uri);
    queryPort = wsIn.outputPort;
    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
    wsOut.setUri(uri);
    queryResultPort = wsOut.input;

    //Set remaining dag options

    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    dag.addStream("InputStream", input.outputPort, dimensions.data);
    dag.addStream("DimensionalData", dimensions.output, adsConverter.inputPort);
    dag.addStream("Converter", adsConverter.outputPort, store.input);
    dag.addStream("Query", queryPort, store.query);
    dag.addStream("QueryResult", store.queryResult, queryResultPort);
  }

  public static class TestDimensions extends DimensionsComputationCustom<AdInfo, AdInfo.AdInfoAggregateEvent>
  {
    public TestDimensions()
    {
    }

    @Override
    public void endWindow()
    {
      LOG.info("endwindow called");

      for(AggregateMap<AdInfo, AdInfo.AdInfoAggregateEvent> map: maps) {

        Set<String> publishers = Sets.newHashSet();
        for(AdInfo.AdInfoAggregateEvent value: map.values()) {
          publishers.add(value.publisher);
          output.emit(value);
        }

        LOG.info("{}", publishers);
        map.clear();
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AdsDimensionsDemoPerformant.class);
}
