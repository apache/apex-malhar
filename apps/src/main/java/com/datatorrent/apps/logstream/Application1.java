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
package com.datatorrent.apps.logstream;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.stream.JsonByteArrayOperator;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;

/**
 *
 * New logstream application.
 * Takes inputs for log types, filters, dimensions and other properties from configuration file and creates multiple
 * operator partitions to cater to those user inputs. It sends the final computation results through the widget output.
 *
 */
public class Application1 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration c)
  {
    int topNtupleCount = 10;
    LogstreamPropertyRegistry registry = new LogstreamPropertyRegistry();
    // set app name
    dag.setAttribute(DAG.APPLICATION_NAME, "Logstream Application");
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 500);

    RabbitMQLogsInputOperator logInput = dag.addOperator("LogInput", new RabbitMQLogsInputOperator());
    logInput.setRegistry(registry);
    logInput.addPropertiesFromString(new String[] {"localhost:5672", "logsExchange", "direct", "apache:mysql:syslog:system"});

    JsonByteArrayOperator jsonToMap = dag.addOperator("JsonToMap", new JsonByteArrayOperator());
    jsonToMap.setConcatenationCharacter('_');

    FilterOperator filterOperator = dag.addOperator("FilterOperator", new FilterOperator());
    filterOperator.setRegistry(registry);
    filterOperator.addFilterCondition(new String[] {"type=apache", "response", "response.equals(\"404\")"});
    filterOperator.addFilterCondition(new String[] {"type=apache", "agentinfo_name", "agentinfo_name.equals(\"Firefox\")"});
    filterOperator.addFilterCondition(new String[] {"type=apache", "default=true"});
    filterOperator.addFilterCondition(new String[] {"type=mysql", "default=true"});
    filterOperator.addFilterCondition(new String[] {"type=syslog", "default=true"});
    filterOperator.addFilterCondition(new String[] {"type=system", "default=true"});

    DimensionOperator dimensionOperator = dag.addOperator("DimensionOperator", new DimensionOperator());
    dimensionOperator.setRegistry(registry);
    String[] dimensionInputString1 = new String[] {"type=apache", "timebucket=s", "dimensions=request", "dimensions=clientip", "dimensions=clientip:request", "values=bytes.sum:bytes.avg"};
    //String[] dimensionInputString1 = new String[] {"type=apache", "timebucket=s", "dimensions=request", "dimensions=clientip","values=bytes.sum"};
    String[] dimensionInputString2 = new String[] {"type=system", "timebucket=s", "dimensions=disk", "values=writes.avg"};
    String[] dimensionInputString3 = new String[] {"type=syslog", "timebucket=s", "dimensions=program", "values=pid.count"};
    dimensionOperator.addPropertiesFromString(dimensionInputString1);
    dimensionOperator.addPropertiesFromString(dimensionInputString2);
    dimensionOperator.addPropertiesFromString(dimensionInputString3);

    LogstreamTopN topN = dag.addOperator("TopN", new LogstreamTopN());
    topN.setN(topNtupleCount);
    topN.setRegistry(registry);

    LogstreamWidgetOutputOperator widgetOut = dag.addOperator("WidgetOut", new LogstreamWidgetOutputOperator());
    widgetOut.logstreamTopNInput.setN(topNtupleCount);
    widgetOut.setRegistry(registry);

    ConsoleOutputOperator consoleOut = dag.addOperator("ConsoleOut", new ConsoleOutputOperator());

    dag.addStream("inputJSonToMap", logInput.outputPort, jsonToMap.input);
    dag.addStream("toFilterOper", jsonToMap.outputFlatMap, filterOperator.input);
    dag.addStream("toDimensionOper", filterOperator.outputMap, dimensionOperator.in);
    dag.addStream("toTopN", dimensionOperator.aggregationsOutput, topN.data);
    dag.addStream("toWS", topN.top, widgetOut.logstreamTopNInput, consoleOut.input);

    dag.setInputPortAttribute(jsonToMap.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterOperator.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(consoleOut.input, PortContext.PARTITION_PARALLEL, true);
  }

}
