/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.nyctaxi;


import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.io.PubSubWebSocketAppDataQuery;
import org.apache.apex.malhar.lib.io.PubSubWebSocketAppDataResult;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.accumulation.SumDouble;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Throwables;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * The DAG definition of the example that illustrates New York City taxi ride data processing.
 *
 * @since 3.8.0
 */
@ApplicationAnnotation(name = "NycTaxiExample")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);
    NycTaxiDataReader inputOperator = new NycTaxiDataReader();
    inputOperator.setDirectory("/user/" + System.getProperty("user.name") + "/nyctaxidata");
    inputOperator.getScanner().setFilePatternRegexp(".*\\.csv$");
    dag.addOperator("NycTaxiDataReader", inputOperator);
    NycTaxiCsvParser parser = dag.addOperator("NycTaxiCsvParser", new NycTaxiCsvParser());
    NycTaxiZipFareExtractor extractor = dag.addOperator("NycTaxiZipFareExtractor", new NycTaxiZipFareExtractor());

    KeyedWindowedOperatorImpl<String, Double, MutableDouble, Double> windowedOperator = new KeyedWindowedOperatorImpl<>();

    // 5-minute windows slide by 1 minute
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.standardMinutes(5)).slideBy(Duration.standardMinutes(1)));

    // Because we only care about the last 5 minutes, and the watermark is set at t-1 minutes, lateness horizon is set to 4 minutes.
    windowedOperator.setAllowedLateness(Duration.standardMinutes(4));
    windowedOperator.setAccumulation(new SumDouble());
    windowedOperator.setTriggerOption(TriggerOption.AtWatermark());
    windowedOperator.setDataStorage(new InMemoryWindowedKeyedStorage<String, MutableDouble>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());

    dag.addOperator("WindowedOperator", windowedOperator);

    NycTaxiDataServer dataServer = dag.addOperator("NycTaxiDataServer", new NycTaxiDataServer());
    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("input_to_parser", inputOperator.output, parser.input);
    dag.addStream("parser_to_extractor", parser.output, extractor.input);
    dag.addStream("extractor_to_windowed", extractor.output, windowedOperator.input);
    dag.addStream("extractor_watermark", extractor.watermarkOutput, windowedOperator.controlInput);
    dag.addStream("windowed_to_console", windowedOperator.output, dataServer.input, console.input);

    PubSubWebSocketAppDataQuery wsQuery = new PubSubWebSocketAppDataQuery();
    wsQuery.enableEmbeddedMode();
    wsQuery.setTopic("nyctaxi.query");
    try {
      wsQuery.setUri(new URI("ws://" + java.net.InetAddress.getLocalHost().getHostName() + ":8890/pubsub"));
    } catch (URISyntaxException | UnknownHostException ex) {
      throw Throwables.propagate(ex);
    }
    dataServer.setEmbeddableQueryInfoProvider(wsQuery);
    PubSubWebSocketAppDataResult wsResult = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
    wsResult.setTopic("nyctaxi.result");
    try {
      wsResult.setUri(new URI("ws://" + java.net.InetAddress.getLocalHost().getHostName() + ":8890/pubsub"));
    } catch (URISyntaxException | UnknownHostException ex) {
      throw Throwables.propagate(ex);
    }
    dag.addStream("server_to_query_output", dataServer.queryResult, wsResult.input);
  }
}
