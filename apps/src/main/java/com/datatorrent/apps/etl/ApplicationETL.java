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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.lib.datamodel.converter.JsonToFlatMapConverter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.sift.Sifter;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

/**
 * ETL Application
 */
public class ApplicationETL implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration c)
  {
    RabbitMQInputOperator<Map<String, Object>> input = dag.addOperator("LogInput", new RabbitMQInputOperator<Map<String, Object>>());
    input.setHost("localhost");
    input.setPort(5672);
    input.setExchange("logsExchange");
    input.setExchangeType("direct");
    input.setLogTypes("apache:mysql:syslog:system");
    input.setConverter(new JsonToFlatMapConverter());

    //Create Predicates for the filters
    Multimap<String, String> filters = ArrayListMultimap.create();
    //apache filters
    filters.put("apache", "response:response.equals(\"404\")");
    filters.put("apache", "agentinfo_name:agentinfo_name.equals(\"Firefox\")");
    //TODO: create Predicates with the above info

    List<Predicate<Map<String, Object>>> predicates = Lists.newArrayList();

    Sifter<Map<String, Object>> sifter = new Sifter<Map<String, Object>>();
    sifter.setPredicates(predicates);

    //Create dimensions specs
    Multimap<String, String> dimensionSpecs = ArrayListMultimap.create();
    //apache dimensions
    dimensionSpecs.put("apache", "time=" + TimeUnit.SECONDS + ":request");
    dimensionSpecs.put("apache", "time=" + TimeUnit.SECONDS + ":clientip");
    dimensionSpecs.put("apache", "time=" + TimeUnit.SECONDS + ":clientip:request");
    //system dimensions
    dimensionSpecs.put("system", "time=" + TimeUnit.SECONDS + ":disk");
    //syslog dimensions
    dimensionSpecs.put("syslog", "time=" + TimeUnit.SECONDS + ":program");

    //Create metrics
    Multimap<String, String> metrics = ArrayListMultimap.create();
    //apache metrics
    metrics.put("apache", "bytes:sum");
    metrics.put("apache", "bytes:avg");
    //system metrics
    metrics.put("system", "writes:avg");
    //syslog metrics
    metrics.put("syslog", "pid:count");
    //TODO: create MetricOperation  instances
    //TODO: create MapAggregator instances using metric metrics
    DimensionsComputationUnifierImpl<Map<String, Object>, MapAggregator.MapAggregateEvent> unifier = new DimensionsComputationUnifierImpl<Map<String, Object>, MapAggregator.MapAggregateEvent>();
    //TODO : set aggregations on unifier

    DimensionsComputation<Map<String, Object>, MapAggregator.MapAggregateEvent> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<Map<String, Object>, MapAggregator.MapAggregateEvent>());
    dimensions.setUnifier(unifier);

    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("Events", input.outputPort, sifter.input);
    dag.addStream("FilteredEvents", sifter.output, dimensions.data);
    dag.addStream("Aggregates", dimensions.output, console.input);
  }
}
