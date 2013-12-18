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
import com.datatorrent.lib.io.ConsoleOutputOperator;

public class TelecomApplication implements StreamingApplication
{

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(TelecomApplication.class);

  private void configureCallForwardingAggregator(CallForwardingAggregatorOperator<String, String> oper)
  {

    Map<String, String> acquirerIdentifier = new HashMap<String, String>();
    acquirerIdentifier.put("callType", "V");
    oper.setAcquirerIdentifier(acquirerIdentifier);

    Map<String, String> mergeeIdentifier = new HashMap<String, String>();
    mergeeIdentifier.put("callType", "U");
    oper.setMergeeIdentifier(mergeeIdentifier);

    List<String> matchFieldList = new ArrayList<String>();
    matchFieldList.add("timeBand");
    matchFieldList.add("callData");
    oper.setMatchFieldList(matchFieldList);

    List<String> mergeFieldList = new ArrayList<String>();
    mergeFieldList.add("ddi");
    oper.setMergeFieldList(mergeFieldList);

    oper.setWindowSize(5);
  }

  private void configureNormalizer(EnrichmentOperator<String, Map<String, String>, String, String> normalizer)
  {
    Map<String, Map<String, String>> prop = new HashMap<String, Map<String, String>>();
    prop.put("callType", new HashMap<String, String>());
    Map<String, String> m = prop.get("callType");
    m.put("V", "Outbound voice call");
    m.put("VOIP", "Voice over IP Call");
    m.put("D", "Data/ISDN call");
    m.put("C", "Conference call");
    prop.put("recording", new HashMap<String, String>());
    Map<String,String> m1 = prop.get("recording");
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

    CallForwardingAggregatorOperator<String, String> aggregator = dag.addOperator("Call Forwarding Aggregator", new CallForwardingAggregatorOperator<String, String>());
    configureCallForwardingAggregator(aggregator);

    EnrichmentOperator<String, String, String, String> enricher = dag.addOperator("Enricher", new EnrichmentOperator<String, String, String, String>());
    enricher.setProp(new HashMap<String, String>());
    enricher.setEnricher(DefaultEnricher.class);

    EnrichmentOperator<String, Map<String, String>, String, String> normalizer = dag.addOperator("Normalizer", new EnrichmentOperator<String, Map<String, String>, String, String>());
    configureNormalizer(normalizer);

    ConsoleOutputOperator console = dag.addOperator("Output", ConsoleOutputOperator.class);
    ConsoleOutputOperator console1 = dag.addOperator("Output1", ConsoleOutputOperator.class);

    dag.addStream("input -> aggregator", input.output, aggregator.input,console1.input);
    dag.addStream("aggregator -> enricher", aggregator.output, enricher.input);
    dag.addStream("enricher -> normalizer", enricher.output, normalizer.input);
    dag.addStream("normalizer -> console", normalizer.output, console.input);
  }

}
