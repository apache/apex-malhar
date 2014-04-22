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
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.contrib.apachelog.ApacheLogInputGenerator;
import com.datatorrent.lib.datamodel.converter.JsonToFlatMapConverter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.fs.TailFsInputOperator;
import com.datatorrent.lib.logs.ApacheLogParseMapOutputOperator;
import com.datatorrent.lib.sift.Sifter;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

/**
 * ETL Application
 */
public class ApplicationETL implements StreamingApplication
{
  private ApacheLogParseMapOutputOperator getParserOperator(DAG dag)
  {
    ApacheLogParseMapOutputOperator parser = dag.addOperator("LogParser", new ApacheLogParseMapOutputOperator());
    parser.setRegexGroups(new String[]{null, "ip", null, "userid", "time", "url", "httpCode", "bytes", null, "agent"});
    parser.setLogRegex("^([\\d\\.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"[A-Z]+ (.+?) HTTP/\\S+\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\".*");
//    GeoIPExtractor geoIPExtractor = new GeoIPExtractor();
//    geoIPExtractor.setDatabasePath("/home/hadoop/GeoLiteCity.dat");
//    parser.registerInformationExtractor("ip", geoIPExtractor);
//    parser.registerInformationExtractor("agent", new UserAgentExtractor());
//    TimestampExtractor timestampExtractor = new TimestampExtractor();
//    timestampExtractor.setDateFormatString("dd/MMM/yyyy:HH:mm:ss Z");
//    parser.registerInformationExtractor("time", timestampExtractor);
    return parser;
  }

  private TailFsInputOperator getTailFSOperator(DAG dag)
  {
    TailFsInputOperator operator = dag.addOperator("TailInput", new TailFsInputOperator());
    //operator.setFilePath(filePath);
    operator.setDelimiter('\n');
    operator.setDelay(1);
    operator.setNumberOfTuples(100);
    return operator;
  }

  private ApacheLogInputGenerator getLogGenerator(DAG dag)
  {
    ApacheLogInputGenerator generator = dag.addOperator("LogGenerator", new ApacheLogInputGenerator());
    generator.setNumberOfTuples(100);
    generator.setMaxDelay(1);
    generator.setIpAddressFile("/home/hadoop/generator/apachelog/ipaddress.txt");
    generator.setUrlFile("/home/hadoop/generator/apachelog/urls.txt");
    generator.setAgentFile("/home/hadoop/generator/apachelog/agents.txt");
    generator.setRefererFile("/home/hadoop/generator/apachelog/referers.txt");
    return generator;
  }

  private RabbitMQInputOperator<Map<String, Object>> getRabbitMQInputOperator(DAG dag)
  {
    RabbitMQInputOperator<Map<String, Object>> input = dag.addOperator("RabbitInput", new RabbitMQInputOperator<Map<String, Object>>());
    input.setHost("localhost");
    input.setPort(5672);
    input.setExchange("logsExchange");
    input.setExchangeType("direct");
    input.setLogTypes("apache:mysql:syslog:system");
    input.setConverter(new JsonToFlatMapConverter());
    return input;
  }

  private DefaultOutputPort<Map<String, Object>> getInputOperatorPort(DAG dag, Configuration configuration)
  {
    String inputType = configuration.get("ETL.Input", "generator");
    if (inputType.equalsIgnoreCase("file")) {
      TailFsInputOperator input = getTailFSOperator(dag);
      ApacheLogParseMapOutputOperator parser = getParserOperator(dag);
      dag.addStream("Generator", input.output, parser.data);
      return parser.output;
    }
    else if (inputType.equalsIgnoreCase("rabbitMQ")) {
      RabbitMQInputOperator<Map<String, Object>> input = getRabbitMQInputOperator(dag);
      return input.output;
    }
    else {
      ApacheLogInputGenerator input = getLogGenerator(dag);
      ApacheLogParseMapOutputOperator parser = getParserOperator(dag);
      dag.addStream("Generator", input.output, parser.data);
      return parser.output;
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration c)
  {
    //Create Predicates for the filters
    Multimap<String, String> filters = ArrayListMultimap.create();
    //apache filters
    filters.put("apache", "httpCode:httpCode.equals(\"404\")");
    filters.put("apache", "browser:browser.equals(\"Firefox\")");
    //TODO: create Predicates with the above info

    List<Predicate<Map<String, Object>>> predicates = Lists.newArrayList();

    Sifter<Map<String, Object>> sifter = dag.addOperator("Sifter", new Sifter<Map<String, Object>>());
    sifter.setPredicates(predicates);

    //Create dimensions specs
    Multimap<String, String> dimensionSpecs = ArrayListMultimap.create();
    //apache dimensions
    dimensionSpecs.put("apache", "time=" + TimeUnit.SECONDS + ":url");
    dimensionSpecs.put("apache", "time=" + TimeUnit.SECONDS + ":ip");
    dimensionSpecs.put("apache", "time=" + TimeUnit.SECONDS + ":ip:url");
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

    dag.addStream("Events", getInputOperatorPort(dag, c), sifter.input);
    dag.addStream("FilteredEvents", sifter.output, dimensions.data);
    dag.addStream("Aggregates", dimensions.output, console.input);
  }
}
