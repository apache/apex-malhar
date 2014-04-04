/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.apps.etl.MapAggregator.MapAggregateEvent;
import com.datatorrent.contrib.db.DataStoreOutputOperator;
import com.datatorrent.lib.datamodel.converter.JsonToFlatMapConverter;
import com.datatorrent.lib.datamodel.operation.CountOperation;
import com.datatorrent.lib.datamodel.operation.SumOperation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputation.Aggregator;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public class AshwinETLApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration c)
  {
    RabbitMQInputOperator<Map<String, Object>> input = dag.addOperator("LogInput", new RabbitMQInputOperator<Map<String, Object>>());
    input.setHost("localhost");
    input.setPort(5672);
    input.setExchange("logsExchange");
    input.setExchangeType("direct");
    input.setLogTypes("apache");
    input.setConverter(new JsonToFlatMapConverter());

    DimensionsComputation<Map<String, Object>, MapAggregator.MapAggregateEvent> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<Map<String, Object>, MapAggregator.MapAggregateEvent>());
    DimensionsComputationUnifierImpl<Map<String, Object>, MapAggregator.MapAggregateEvent> unifier = new DimensionsComputationUnifierImpl<Map<String, Object>, MapAggregator.MapAggregateEvent>();
    dimensions.setUnifier(unifier);

    List<Metric> apacheMetrics = new ArrayList<Metric>();
    Metric metric1 = new Metric("bytes", "bytesCount", new CountOperation());
    Metric metric2 = new Metric("bytes", "bytesSum", new SumOperation());

    apacheMetrics.add(metric1);
    apacheMetrics.add(metric2);

    MapAggregator apacheDimension1 = new MapAggregator();
    apacheDimension1.init("clientip", apacheMetrics);

    MapAggregator apacheDimension2 = new MapAggregator();
    apacheDimension2.init("clientip:geoip_country_name", apacheMetrics);

    MapAggregator[] aggrs = new MapAggregator[] {apacheDimension1, apacheDimension2};
    dimensions.setAggregators(aggrs);

    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());
    console.silent = true;

    DataStoreOutputOperator<MapAggregateEvent, MapAggregateEvent> storeOutputOperator = dag.addOperator("StoreOutputOperator", new DataStoreOutputOperator<MapAggregateEvent, MapAggregateEvent>());

    MongoDBMapAggregateWriter storeWriter = new MongoDBMapAggregateWriter();
    storeWriter.setHostName("localhost");
    storeWriter.setDataBase("etl");
    storeWriter.setTable("apacheAggregates");
    storeWriter.setAggregators(aggrs);
    storeOutputOperator.setStore(storeWriter);

    dag.addStream("Events", input.outputPort, dimensions.data);
    dag.addStream("Aggregates", dimensions.output, storeOutputOperator.input, console.input);

    dag.setInputPortAttribute(console.input, PortContext.PARTITION_PARALLEL, true);
  }

}
