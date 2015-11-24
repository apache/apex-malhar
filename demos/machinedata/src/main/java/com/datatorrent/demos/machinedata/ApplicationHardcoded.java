/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.dimensions.DimensionStoreHDHTNonEmptyQueryResultUnifier;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.demos.machinedata.data.MachineAggregatorHardCodedCount;
import com.datatorrent.demos.machinedata.data.MachineAggregatorHardCodedSum;
import com.datatorrent.demos.machinedata.data.MachineHardCodedAggregate;
import com.datatorrent.demos.machinedata.data.MachineHardCodedAggregateConverter;
import com.datatorrent.demos.machinedata.data.MachineInfo;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

@ApplicationAnnotation(name = ApplicationHardcoded.APP_NAME)
/**
 * @since 3.2.0
 */
public class ApplicationHardcoded implements StreamingApplication
{
  public static final String APP_NAME = "MachineDataDemoHardcoded";
  public static final String EVENT_SCHEMA = "machinedataschema.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String propStorePath = "dt.application." + APP_NAME + ".operator.Store.fileStore.basePathPrefix";
    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);

    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
    DimensionalConfigurationSchema configurationSchema = new DimensionalConfigurationSchema(eventSchema,
                                                                                            AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    List<DimensionsDescriptor> dimensionsDescriptors = Lists.newArrayList();

    for (int counter = 0; counter < configurationSchema.getDimensionsDescriptorToID().size(); counter++) {
      dimensionsDescriptors.add(null);
    }

    for (Map.Entry<DimensionsDescriptor, Integer> entry : configurationSchema.getDimensionsDescriptorToID().entrySet()) {
      dimensionsDescriptors.set(entry.getValue(), entry.getKey());
    }

    @SuppressWarnings({"unchecked", "MismatchedReadAndWriteOfArray", "rawtypes"})
    DimensionsComputation.Aggregator<MachineInfo, MachineHardCodedAggregate>[] aggregators
      = (DimensionsComputation.Aggregator<MachineInfo, MachineHardCodedAggregate>[])new DimensionsComputation.Aggregator[dimensionsDescriptors.size() * 2];
    for (int ddID = 0, aggregatorIndex = 0; ddID < dimensionsDescriptors.size(); ddID++) {
      DimensionsDescriptor dimensionsDescriptor = dimensionsDescriptors.get(ddID);
      aggregators[aggregatorIndex] = new MachineAggregatorHardCodedSum(ddID, dimensionsDescriptor);
      aggregatorIndex++;
      aggregators[aggregatorIndex] = new MachineAggregatorHardCodedCount(ddID, dimensionsDescriptor);
      aggregatorIndex++;
    }

    InputReceiver randomGen = dag.addOperator("Receiver", InputReceiver.class);
    randomGen.setEventSchema(eventSchema);

    DimensionsComputation<MachineInfo, MachineHardCodedAggregate> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<MachineInfo, MachineHardCodedAggregate>());
    dimensions.setAggregators(aggregators);
    dag.getMeta(dimensions).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 6);
    dag.getMeta(dimensions).getAttributes().put(OperatorContext.CHECKPOINT_WINDOW_COUNT, 6);
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", new AppDataSingleSchemaDimensionStoreHDHT());
    store.setCacheWindowDuration(120);
    @SuppressWarnings("unchecked")
    MachineHardCodedAggregateConverter converter = dag.addOperator("Converter", MachineHardCodedAggregateConverter.class);
    converter.setConfigurationSchemaJSON(eventSchema);

    DimensionsComputationUnifierImpl<MachineInfo, MachineHardCodedAggregate> unifier = new DimensionsComputationUnifierImpl<>();
    unifier.setAggregators(aggregators);
    dimensions.setUnifier(unifier);
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
    dag.addStream("InputStream", randomGen.outputInline, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, converter.input);
    dag.addStream("ConvertedData", converter.output, store.input);
    dag.addStream("QueryResult", store.queryResult, wsOut.input);
  }

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationHardcoded.class);
}
