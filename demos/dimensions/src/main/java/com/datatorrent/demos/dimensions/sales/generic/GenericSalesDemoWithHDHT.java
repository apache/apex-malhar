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
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.dimensions.GenericAppDataDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.contrib.kafka.KafkaJsonEncoder;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;

import static com.datatorrent.demos.dimensions.ads.generic.GenericApplicationWithHDHT.*;

import com.datatorrent.demos.dimensions.ads.generic.GenericApplicationWithHDHT;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@ApplicationAnnotation(name=GenericSalesDemoWithHDHT.APP_NAME)
public class GenericSalesDemoWithHDHT implements StreamingApplication
{
  private static final Logger logger = LoggerFactory.getLogger(GenericSalesDemoWithHDHT.class);

  public static final String APP_NAME = "GenericSalesDemoWithHDHT";
  public static final String PROP_USE_WEBSOCKETS = "dt.application." + APP_NAME + ".useWebSockets";
  public static final String PROP_STORE_PATH = "dt.application." + APP_NAME + ".operator.Store.fileStore.basePathPrefix";

  public GenericSalesDemoWithHDHT()
  {
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("InputGenerator", JsonSalesGenerator.class);
    JsonToMapConverter converter = dag.addOperator("Converter", JsonToMapConverter.class);
    GenericSalesDimensionComputation dimensions = dag.addOperator("DimensionsComputation", GenericSalesDimensionComputation.class);

    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    GenericAppDataDimensionStoreHDHT store = dag.addOperator("Store", GenericAppDataDimensionStoreHDHT.class);

    String basePath = conf.get(PROP_STORE_PATH);
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();

    if(basePath != null) {
      basePath += System.currentTimeMillis();
      hdsFile.setBasePath(basePath);
      System.out.println("Setting basePath " + basePath);
    }

    store.setFileStore(hdsFile);
    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator< MutableLong >());

    logger.info("Before reading schemas.");
    StringWriter eventWriter = new StringWriter();
    try {
      IOUtils.copy(GenericApplicationWithHDHT.class.getClassLoader().getResourceAsStream(EVENT_SCHEMA),
                   eventWriter);
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    String eventSchema = eventWriter.toString();

    StringWriter dimensionalWriter = new StringWriter();
    try {
      IOUtils.copy(GenericApplicationWithHDHT.class.getClassLoader().getResourceAsStream(DIMENSIONAL_SCHEMA),
                   dimensionalWriter);
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    String dimensionalSchema = dimensionalWriter.toString();

    logger.info("After reading schemas.");
    logger.info("Event Schema: {}");
    logger.info("Dimensions Schema: {}");

    dimensions.setEventSchemaJSON(eventSchema);
    store.setEventSchemaJSON(eventSchema);
    store.setDimensionalSchemaJSON(dimensionalSchema);
    input.setDataSchemaJSON(dimensionalSchema);

    Operator.OutputPort<String> queryPort;
    Operator.InputPort<String> queryResultPort;
    if (conf.getBoolean(PROP_USE_WEBSOCKETS,  false)) {
      String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
      URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
      //LOG.info("WebSocket with gateway at: {}", gatewayAddress);
      PubSubWebSocketAppDataQuery wsIn = dag.addOperator("Query", new PubSubWebSocketAppDataQuery());
      wsIn.setUri(uri);
      queryPort = wsIn.outputPort;
      PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
      wsOut.setUri(uri);
      queryResultPort = wsOut.input;
    } else {
      KafkaSinglePortStringInputOperator queries = dag.addOperator("Query", new KafkaSinglePortStringInputOperator());
      queries.setConsumer(new SimpleKafkaConsumer());
      queryPort = queries.outputPort;
      KafkaSinglePortOutputOperator<String, String> queryResult = dag.addOperator("QueryResult", new KafkaSinglePortOutputOperator<String, String>());
      queryResult.getConfigProperties().put("serializer.class", KafkaJsonEncoder.class.getName());
      queryResultPort = queryResult.inputPort;
    }

    dag.addStream("InputStream", input.jsonBytes, converter.input);
    dag.addStream("ConvertStream", converter.outputMap, dimensions.inputEvent);
    dag.addStream("DimensionalData", dimensions.aggregateOutput, store.input);

    dag.addStream("Query", queryPort, store.query).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("QueryResult", store.queryResult, queryResultPort).setLocality(Locality.CONTAINER_LOCAL);
  }
}
