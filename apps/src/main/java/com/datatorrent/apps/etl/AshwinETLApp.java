/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.etl;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.apps.etl.MapAggregator.MapAggregateEvent;
import com.datatorrent.contrib.db.DataStoreOutputOperator;
import com.datatorrent.lib.datamodel.converter.JsonToFlatMapConverter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

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

    MapAggregator[] aggrs = getAggregators();
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

  private MapAggregator[] getAggregators() {
//    Metric count = new Metric("bytes", "bytesCount", new CountOperation());
//    Metric bytesSum = new Metric("bytes", "bytesSum", new SumOperation());
//
//    List<Metric> countAndSumOps = new ArrayList<Metric>();
//    countAndSumOps.add(count);
//    countAndSumOps.add(bytesSum);
//
//    List<Metric> countOps = new ArrayList<Metric>();
//    countOps.add(count);
//
//    MapAggregator apacheDimension1 = new MapAggregator();
//    apacheDimension1.init("geoip_country_name", countOps);
//
//    MapAggregator apacheDimension2 = new MapAggregator();
//    apacheDimension2.init("geoip_city_name", countOps);
//
//    MapAggregator apacheDimension3 = new MapAggregator();
//    apacheDimension3.init("geoip_region_name", countOps);
//
//    MapAggregator apacheDimension4 = new MapAggregator();
//    apacheDimension4.init("agentinfo_name", countOps); // browser
//
//    MapAggregator apacheDimension5 = new MapAggregator();
//    apacheDimension5.init("agentinfo_os", countOps);
//
//    MapAggregator apacheDimension6 = new MapAggregator();
//    apacheDimension6.init("agentinfo_device", countOps);
//
//    MapAggregator apacheDimension7 = new MapAggregator();
//    apacheDimension7.init("request", countOps);
//
//    MapAggregator apacheDimension8 = new MapAggregator();
//    apacheDimension8.init("clientip", countAndSumOps);
//
//    MapAggregator apacheDimension9 = new MapAggregator();
//    apacheDimension9.init("geoip_country_name:request", countOps);
//
//    MapAggregator apacheDimension10 = new MapAggregator();
//    apacheDimension10.init("agentinfo_name:request", countOps);
//
//    MapAggregator apacheDimension11 = new MapAggregator();
//    apacheDimension11.init("agentinfo_device:request", countOps);
//
//    MapAggregator apacheDimension12 = new MapAggregator();
//    apacheDimension12.init("geoip_country_name:request", countOps);
//
//    MapAggregator apacheDimension13 = new MapAggregator();
//    apacheDimension13.init("clientip:geoip_country_name", countOps);
//
//    MapAggregator apacheDimension14 = new MapAggregator();
//    apacheDimension14.init("timestamp=" + TimeUnit.DAYS, countOps);
//
//    MapAggregator apacheDimension15 = new MapAggregator();
//    apacheDimension15.init("timestamp=" + TimeUnit.DAYS + ":agentinfo_device", countOps);
//
//    MapAggregator[] aggrs = new MapAggregator[]
//    {
//      apacheDimension1,
//      apacheDimension2,
//      apacheDimension3,
//      apacheDimension4,
//      apacheDimension5,
//      apacheDimension6,
//      apacheDimension7,
//      apacheDimension8,
//      apacheDimension9,
//      apacheDimension10,
//      apacheDimension11,
//      apacheDimension12,
//      apacheDimension13,
//      apacheDimension14,
//      apacheDimension15
//    };
//
//    return aggrs;
    return null;
  }
}
