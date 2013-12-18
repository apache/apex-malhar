/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.apps.telecom;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.apps.telecom.operator.CallForwardingAggregatorOperator;
import com.datatorrent.apps.telecom.operator.DefaultEnricher;
import com.datatorrent.apps.telecom.operator.DefaultNormalizer;
import com.datatorrent.apps.telecom.operator.EnrichmentOperator;
import com.datatorrent.apps.telecom.operator.InputGenerator;
import com.datatorrent.apps.telecom.util.TestHeaderMapping;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.parser.CsvParserOperator;
/**
 *  @since 0.9.2
 * @author gaurav gupta <gaurav@datatorrent.com>
 *
 */
public class TelecomApplication implements StreamingApplication
{

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(TelecomApplication.class);

  private void configureCallForwardingAggregator(CallForwardingAggregatorOperator<String, Object> oper)
  {

    Map<String, Object> acquirerIdentifier = new HashMap<String, Object>();
    acquirerIdentifier.put("callType", "V");
    oper.setAcquirerIdentifier(acquirerIdentifier);

    Map<String, Object> mergeeIdentifier = new HashMap<String, Object>();
    mergeeIdentifier.put("callType", "U");
    oper.setMergeeIdentifier(mergeeIdentifier);

    List<String> matchFieldList = new ArrayList<String>();
    matchFieldList.add("callDate");
    oper.setMatchFieldList(matchFieldList);

    List<String> mergeFieldList = new ArrayList<String>();
    mergeFieldList.add("ddi");
    oper.setMergeFieldList(mergeFieldList);

    oper.setWindowSize(5);
  }

  private void configureNormalizer(EnrichmentOperator<String, Map<String, Object>, String, Object> normalizer)
  {
    Map<String, Map<String, Object>> prop = new HashMap<String, Map<String, Object>>();
    prop.put("callType", new HashMap<String, Object>());
    Map<String, Object> m = prop.get("callType");
    m.put("V", "Outbound voice call");
    m.put("VOIP", "Voice over IP Call");
    m.put("D", "Data/ISDN call");
    m.put("C", "Conference call");
    prop.put("recording", new HashMap<String, Object>());
    Map<String, Object> m1 = prop.get("recording");
    m1.put("1", "Recorded");
    m1.put("0", "Not Recorded");
    m1.put("", "Not Recorded");
    normalizer.setProp(prop);

    normalizer.setEnricher(DefaultNormalizer.class);
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    InputGenerator input = dag.addOperator("input", InputGenerator.class);

    CsvParserOperator parser = dag.addOperator("parser", CsvParserOperator.class);
    TestHeaderMapping headerMapping = new TestHeaderMapping();
    headerMapping.setHeaders(new String[] { "callType", "callCause", "cli", "telephone", "callDate", "callTime", "duration", "bytesTransmitted", "bytesReceived", "description", "chargeCode", "timeBand", "salesPrice", "preBundle", "extension", "ddi", "groupingId", "callClass", "carrier", "recording", "vat", "countryOfOrigin", "network", "retailTariffCode", "remoteNetwork", "apn", "divertedNumber", "ringTime", "recordId" });
    parser.setHeaderMapping(headerMapping);

    CallForwardingAggregatorOperator<String, Object> aggregator = dag.addOperator("Call Forwarding Aggregator", new CallForwardingAggregatorOperator<String, Object>());
    configureCallForwardingAggregator(aggregator);

    EnrichmentOperator<String, String, String, Object> enricher = dag.addOperator("Enricher", new EnrichmentOperator<String, String, String, Object>());
    enricher.setProp(new HashMap<String, String>());
    enricher.setEnricher(DefaultEnricher.class);

    EnrichmentOperator<String, Map<String, Object>, String, Object> normalizer = dag.addOperator("Normalizer", new EnrichmentOperator<String, Map<String, Object>, String, Object>());
    configureNormalizer(normalizer);

    ConsoleOutputOperator console = dag.addOperator("Output", ConsoleOutputOperator.class);
    ConsoleOutputOperator console1 = dag.addOperator("Output1", ConsoleOutputOperator.class);

    dag.addStream("input -> parser", input.output, parser.stringInput, console1.input);
    dag.addStream("parser -> aggregator", parser.mapOutput, aggregator.input);
    dag.addStream("aggregator -> enricher", aggregator.output, enricher.input);
    dag.addStream("enricher -> normalizer", enricher.output, normalizer.input);
    dag.addStream("normalizer -> console", normalizer.output, console.input);
  }

}
