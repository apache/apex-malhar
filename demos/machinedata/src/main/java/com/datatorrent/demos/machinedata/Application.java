/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.dimensions.DimensionStoreHDHTNonEmptyQueryResultUnifier;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

@ApplicationAnnotation(name=Application.APP_NAME)
/**
 * @since 3.2.0
 */
public class Application implements StreamingApplication
{
  public static final String EVENT_SCHEMA = "machinedataschema.json";
  public static final String APP_NAME = "MachineDataDemo";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String propStorePath = "dt.application." + APP_NAME + ".operator.Store.fileStore.basePathPrefix";

    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);
    InputReceiver randomGen = dag.addOperator("Receiver", InputReceiver.class);
    randomGen.setEventSchema(eventSchema);

    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("DimensionsComputation", getDimensions());
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 6);
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 6);
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", getStore());
    store.setCacheWindowDuration(120);
    @SuppressWarnings("unchecked")
    PassThroughOperator unifier = dag.addOperator("Unifier", PassThroughOperator.class);

    //Set operator properties

    Map<String, String> keyToExpression = Maps.newHashMap();
    keyToExpression.put("Customer ID", "getMachineKey().getCustomer()");
    keyToExpression.put("Product ID", "getMachineKey().getProduct()");
    keyToExpression.put("Product OS", "getMachineKey().getOs()");
    keyToExpression.put("Software1 Ver", "getMachineKey().getSoftware1()");
    keyToExpression.put("Software2 Ver", "getMachineKey().getSoftware2()");
    keyToExpression.put("Device ID", "getMachineKey().getDeviceId()");
    keyToExpression.put("time", "getMachineKey().getTimestamp()");

    Map<String, String> aggregateToExpression = Maps.newHashMap();
    aggregateToExpression.put("CPU Usage (%)", "getCpu()");
    aggregateToExpression.put("RAM Usage (%)", "getRam()");
    aggregateToExpression.put("HDD Usage (%)", "getHdd()");

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
    store.setFileStore(hdsFile);
    store.getResultFormatter().setContinuousFormatString("#.00");
    store.setConfigurationSchemaJSON(eventSchema);
    store.setQueryResultUnifier(new DimensionStoreHDHTNonEmptyQueryResultUnifier());

    store.setEmbeddableQueryInfoProvider(new PubSubWebSocketAppDataQuery());
    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());

    //Set remaining dag options

    dag.addStream("InputStream", randomGen.outputInline, dimensions.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, unifier.input);
    dag.addStream("Unifier", unifier.output, store.input);
    dag.addStream("QueryResult", store.queryResult, wsOut.input);
  }

  public DimensionsComputationFlexibleSingleSchemaPOJO getDimensions()
  {
    return new DimensionsComputationFlexibleSingleSchemaPOJO();
  }

  public AppDataSingleSchemaDimensionStoreHDHT getStore()
  {
    return new AppDataSingleSchemaDimensionStoreHDHT();
  }

  public static class PassThroughOperator extends BaseOperator
  {
    public final transient DefaultInputPort<Aggregate> input = new DefaultInputPort<Aggregate>() {

      @Override
      public void process(Aggregate tuple)
      {
        output.emit(tuple);
      }
    };

    public final transient DefaultOutputPort<Aggregate> output = new DefaultOutputPort<>();
  }
}
