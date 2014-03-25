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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.apps.logstream.LogstreamTopN;
import com.datatorrent.apps.logstream.LogstreamWidgetOutputOperator;
import com.datatorrent.lib.datamodel.converter.JsonToFlatMapConverter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.statistics.DimensionsComputation;

/**
 * New logstream application.
 * Takes inputs for log types, filters, dimensions and other properties from configuration file and creates multiple
 * operator partitions to cater to those user inputs. It sends the final computation results through the widget output.
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

    //Create filters
    Multimap<String, String> filters = ArrayListMultimap.create();
    //apache filters
    filters.put("apache", "response:response.equals(\"404\")");
    filters.put("apache", "agentinfo_name:agentinfo_name.equals(\"Firefox\")");
    filters.put("apache", Constants.DEFAULT_FILTER);
    //mysql filters
    filters.put("mysql", Constants.DEFAULT_FILTER);
    //syslog filters
    filters.put("syslog", Constants.DEFAULT_FILTER);
    //system filters
    filters.put("system", Constants.DEFAULT_FILTER);

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
    //TODO: create DimensionsAggregator instances using metric metrics
    ComputationsUnifier unifier = new ComputationsUnifier();
    //TODO : set aggregations on unifier

    DimensionsComputation<Map<String, Object>> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<Map<String, Object>>());
    dimensions.setUnifier(unifier);

    int topNtupleCount = 10;

    LogstreamTopN topN = dag.addOperator("TopN", new LogstreamTopN());
    topN.setN(topNtupleCount);

    LogstreamWidgetOutputOperator widgetOut = dag.addOperator("WidgetOut", new LogstreamWidgetOutputOperator());
    widgetOut.logstreamTopNInput.setN(topNtupleCount);

    ConsoleOutputOperator consoleOut = dag.addOperator("ConsoleOut", new ConsoleOutputOperator());

    dag.addStream("Events", input.outputPort, dimensions.data);
//    dag.addStream("Aggregates", dimensions.output, topN.data);
    dag.addStream("toWS", topN.top, widgetOut.logstreamTopNInput, consoleOut.input);

    dag.setInputPortAttribute(consoleOut.input, PortContext.PARTITION_PARALLEL, true);
  }
}
