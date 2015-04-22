/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.sales.generic;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.appdata.dimensions.DimensionsComputationSingleSchemaMap;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import java.net.URI;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SalesDemoWithHDHT implements StreamingApplication
{
  private static final Logger logger = LoggerFactory.getLogger(SalesDemoWithHDHT.class);

  public static final String APP_NAME = "GenericSalesDemoWithHDHT";
  public static final String PROP_USE_WEBSOCKETS = "dt.application." + APP_NAME + ".useWebSockets";
  public static final String PROP_STORE_PATH = "dt.application." + APP_NAME + ".operator.Store.fileStore.basePathPrefix";

  public static final String EVENT_SCHEMA = "salesGenericEventSchema.json";
  public static final String DIMENSIONAL_SCHEMA = "salesGenericDataSchema.json";

  public SalesDemoWithHDHT()
  {
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("InputGenerator", JsonSalesGenerator.class);
    JsonToMapConverter converter = dag.addOperator("Converter", JsonToMapConverter.class);
    DimensionsComputationSingleSchemaMap dimensions =
    dag.addOperator("DimensionsComputation", DimensionsComputationSingleSchemaMap.class);

    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);

    String basePath = conf.get(PROP_STORE_PATH);
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();

    if(basePath != null) {
      basePath += System.currentTimeMillis();
      hdsFile.setBasePath(basePath);
      System.out.println("Setting basePath " + basePath);
    }

    store.setFileStore(hdsFile);
    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator< MutableLong >());
    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);
    String dimensionalSchema = SchemaUtils.jarResourceFileToString(DIMENSIONAL_SCHEMA);

    logger.info("After reading schemas.");
    logger.info("Event Schema: {}");
    logger.info("Dimensions Schema: {}");

    dimensions.setEventSchemaJSON(eventSchema);
    store.setEventSchemaJSON(eventSchema);
    store.setDimensionalSchemaJSON(dimensionalSchema);
    input.setEventSchemaJSON(eventSchema);

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

    dag.addStream("InputStream", input.jsonBytes, converter.input);
    dag.addStream("ConvertStream", converter.outputMap, dimensions.inputEvent);
    dag.addStream("DimensionalData", dimensions.aggregateOutput, store.input);

    dag.addStream("Query", queryPort, store.query).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("QueryResult", store.queryResult, queryResultPort).setLocality(Locality.CONTAINER_LOCAL);
  }
}
