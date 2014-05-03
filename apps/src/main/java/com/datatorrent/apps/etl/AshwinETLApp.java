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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;


import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.logs.ApacheLogParseMapOutputOperator;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.apps.etl.Chart.CHART_TYPE;
import com.datatorrent.apps.etl.Chart.LineChartParams;

import com.datatorrent.contrib.apachelog.ApacheLogInputGenerator;
import com.datatorrent.contrib.apachelog.GeoIPExtractor;
import com.datatorrent.contrib.apachelog.TimestampExtractor;
import com.datatorrent.contrib.apachelog.UserAgentExtractor;

public class AshwinETLApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration c)
  {
    setLibraryJars(dag);
    /*
     RabbitMQInputOperator<Map<String, Object>> lineChartInput = dag.addOperator("LogInput", new RabbitMQInputOperator<Map<String, Object>>());
     lineChartInput.setHost("localhost");
     lineChartInput.setPort(5672);
     lineChartInput.setExchange("logsExchange");
     lineChartInput.setExchangeType("direct");
     lineChartInput.setLogTypes("apache");
     lineChartInput.setConverter(new JsonToFlatMapConverter());
     */

    ApacheLogInputGenerator generator = dag.addOperator("LogGenerator", new ApacheLogInputGenerator());
    generator.setIpAddressFile("/tmp/test/apachelog/ipaddress.txt");
    generator.setUrlFile("/tmp/test/apachelog/urls.txt");
    generator.setAgentFile("/tmp/test/apachelog/agents.txt");
    generator.setRefererFile("/tmp/test/apachelog/referers.txt");
    ApacheLogParseMapOutputOperator parser = getParserOperator(dag);

    DimensionsComputation<Map<String, Object>, MapAggregator.MapAggregateEvent> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<Map<String, Object>, MapAggregator.MapAggregateEvent>());
    DimensionsComputationUnifierImpl<Map<String, Object>, MapAggregator.MapAggregateEvent> unifier = new DimensionsComputationUnifierImpl<Map<String, Object>, MapAggregator.MapAggregateEvent>();
    dimensions.setUnifier(unifier);

    MapAggregator[] aggrs = getAggregators();
    dimensions.setAggregators(aggrs);
    unifier.setAggregators(aggrs);

    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());
    //console.silent = true;

    MongoDBMapAggregateWriter storeWriter = new MongoDBMapAggregateWriter();
    //storeWriter.setHostName("localhost:8003"); // forward to node17
    //storeWriter.setDataBase("etl_sample2");
    storeWriter.setHostName("localhost:27017"); // localhost
    storeWriter.setDataBase("etl3");
    storeWriter.setTable("aggregations");
    storeWriter.setAggregators(aggrs);

    /*
     DataStoreOutputOperator<MapAggregateEvent, MapAggregateEvent> storeOutputOperator = dag.addOperator("StoreOutputOperator", new DataStoreOutputOperator<MapAggregateEvent, MapAggregateEvent>());
     storeOutputOperator.setStore(storeWriter);
     */

    AggregationsOperator aggregationsOperator = dag.addOperator("AggregationsOpeator", new AggregationsOperator());
    aggregationsOperator.setAggregators(aggrs);
    aggregationsOperator.setStore(storeWriter);

    LineChartMapAggregateOperator lineChartOper = dag.addOperator("XYLineChartOperator", new LineChartMapAggregateOperator());
    lineChartOper.setAggregators(aggrs);
    lineChartOper.setChartParamsList(getLineChartParams());

    RealTimeChartOutputOperator wsout = dag.addOperator("WSOutOper", new RealTimeChartOutputOperator());
    wsout.setLineChartParamsList(getLineChartParams());

    //dag.addStream("Events", lineChartInput.output, dimensions.data);
    dag.addStream("Generator", generator.output, parser.input);
    dag.addStream("Events", parser.output, dimensions.data);
    dag.addStream("Aggregates", dimensions.output, aggregationsOperator.input);
    dag.addStream("lineChart", aggregationsOperator.output, lineChartOper.input);
    dag.addStream("WsOut", lineChartOper.outputPort, wsout.lineChartInput, console.input);

    dag.setInputPortAttribute(console.input, PortContext.PARTITION_PARALLEL, true);
  }

  private MapAggregator[] getAggregators()
  {
    /*
     Metric count = new Metric("bytes", "count", new CountOperation());
     Metric bytesSum = new Metric("bytes", "bandwidth", new SumOperation());

     List<Metric> countAndSumOps = new ArrayList<Metric>();
     countAndSumOps.add(count);
     countAndSumOps.add(bytesSum);

     List<Metric> countOps = new ArrayList<Metric>();
     countOps.add(count);
     */

    MapAggregator apacheDimension1 = new MapAggregator();
    //apacheDimension1.init("geoip_country_name", countAndSumOps);
    apacheDimension1.init("geoip_country_name");

    MapAggregator apacheDimension2 = new MapAggregator();
    //apacheDimension2.init("geoip_city_name", countOps);
    apacheDimension2.init("geoip_city_name");

    MapAggregator apacheDimension3 = new MapAggregator();
    //apacheDimension3.init("geoip_region_name", countOps);
    apacheDimension3.init("geoip_region_name");

    MapAggregator apacheDimension4 = new MapAggregator();
    //apacheDimension4.init("agentinfo_name", countOps); // browser
    apacheDimension4.init("agentinfo_name"); // browser

    MapAggregator apacheDimension5 = new MapAggregator();
    //apacheDimension5.init("agentinfo_os", countOps);
    apacheDimension5.init("agentinfo_os");

    MapAggregator apacheDimension6 = new MapAggregator();
    //apacheDimension5.init("agentinfo_os", countOps);
    apacheDimension6.init("agentinfo_device");

    MapAggregator apacheDimension7 = new MapAggregator();
    //apacheDimension7.init("request", countOps);
    apacheDimension7.init("request");

    MapAggregator apacheDimension8 = new MapAggregator();
    //apacheDimension8.init("clientip", countAndSumOps);
    apacheDimension8.init("clientip");

    MapAggregator apacheDimension9 = new MapAggregator();
    //apacheDimension9.init("geoip_country_name:request", countOps);
    apacheDimension9.init("geoip_country_name:request");

    MapAggregator apacheDimension10 = new MapAggregator();
    //apacheDimension10.init("agentinfo_name:request", countOps);
    apacheDimension10.init("agentinfo_name:request");

    MapAggregator apacheDimension11 = new MapAggregator();
    //apacheDimension11.init("agentinfo_device:request", countOps);
    apacheDimension11.init("agentinfo_device:request");

    MapAggregator apacheDimension12 = new MapAggregator();
    //apacheDimension12.init("geoip_country_name:request", countOps);
    apacheDimension12.init("geoip_country_name:request");

    MapAggregator apacheDimension13 = new MapAggregator();
    //apacheDimension13.init("clientip:geoip_country_name", countOps);
    apacheDimension13.init("clientip:geoip_country_name");

    MapAggregator apacheDimension14 = new MapAggregator();
    //apacheDimension14.init("timestamp=" + TimeUnit.DAYS, countOps);
    apacheDimension14.init("timestamp=" + TimeUnit.DAYS);

    MapAggregator apacheDimension15 = new MapAggregator();
    //apacheDimension15.init("timestamp=" + TimeUnit.DAYS + ":agentinfo_device", countOps);
    apacheDimension15.init("timestamp=" + TimeUnit.DAYS + ":agentinfo_device");

    MapAggregator apacheDimension16 = new MapAggregator();
    //apacheDimension16.init("timestamp=" + TimeUnit.DAYS + ":request", countOps);
    apacheDimension16.init("timestamp=" + TimeUnit.DAYS + ":request");

    MapAggregator apacheDimension17 = new MapAggregator();
    //apacheDimension17.init("timestamp=" + TimeUnit.DAYS + ":geoip_country_name:request", countOps);
    apacheDimension17.init("timestamp=" + TimeUnit.DAYS + ":geoip_country_name:request");

    MapAggregator apacheDimension18 = new MapAggregator();
    //apacheDimension18.init("timestamp=" + TimeUnit.HOURS, countAndSumOps);
    apacheDimension18.init("timestamp=" + TimeUnit.HOURS);

    MapAggregator apacheDimension19 = new MapAggregator();
    //apacheDimension19.init("timestamp=" + TimeUnit.HOURS + ":agentinfo_device", countOps);
    apacheDimension19.init("timestamp=" + TimeUnit.HOURS + ":agentinfo_device");

    MapAggregator apacheDimension20 = new MapAggregator();
    //apacheDimension20.init("timestamp=" + TimeUnit.HOURS + ":request", countOps);
    apacheDimension20.init("timestamp=" + TimeUnit.HOURS + ":request");

    MapAggregator apacheDimension21 = new MapAggregator();
    //apacheDimension21.init("timestamp=" + TimeUnit.HOURS + ":geoip_country_name:request", countOps);
    apacheDimension21.init("timestamp=" + TimeUnit.HOURS + ":geoip_country_name:request");

    MapAggregator apacheDimension22 = new MapAggregator();
    //apacheDimension22.init("timestamp=" + TimeUnit.MINUTES, countAndSumOps);
    apacheDimension22.init("timestamp=" + TimeUnit.MINUTES);

    MapAggregator apacheDimension23 = new MapAggregator();
    //apacheDimension23.init("timestamp=" + TimeUnit.MINUTES + ":agentinfo_device", countOps);
    apacheDimension23.init("timestamp=" + TimeUnit.MINUTES + ":agentinfo_device");

    MapAggregator apacheDimension24 = new MapAggregator();
    //apacheDimension24.init("timestamp=" + TimeUnit.MINUTES + ":request", countOps);
    apacheDimension24.init("timestamp=" + TimeUnit.MINUTES + ":request");

    MapAggregator apacheDimension25 = new MapAggregator();
    //apacheDimension25.init("timestamp=" + TimeUnit.MINUTES + ":geoip_country_name:request", countOps);
    apacheDimension25.init("timestamp=" + TimeUnit.MINUTES + ":geoip_country_name:request");

    MapAggregator apacheDimension26 = new MapAggregator();
    //apacheDimension26.init("timestamp=" + TimeUnit.MINUTES + ":geoip_country_name", countAndSumOps);
    apacheDimension26.init("timestamp=" + TimeUnit.MINUTES + ":geoip_country_name");

    MapAggregator[] aggrs = new MapAggregator[] {
      /*
       apacheDimension1,
       apacheDimension2,
       apacheDimension3,
       apacheDimension4,
       apacheDimension5,
       apacheDimension6,
       apacheDimension7,
       apacheDimension8,
       apacheDimension9,
       apacheDimension10,
       apacheDimension11,
       apacheDimension12,
       apacheDimension13,
       apacheDimension14,
       apacheDimension15,
       apacheDimension16,
       apacheDimension17,
       */
      apacheDimension18,
      //apacheDimension19,
      //apacheDimension20,
      //apacheDimension21,
      apacheDimension22,
      //apacheDimension23,
      //apacheDimension24,
      //apacheDimension25,
      apacheDimension26
    };

    return aggrs;

    //return null;
  }

  public LineChartParams[] getLineChartParams()
  {

    ArrayList<String> metrics = new ArrayList<String>();
    metrics.add(Constants.BYTES_SUM_DEST);
    metrics.add(Constants.COUNT_DEST);
    //metrics.add(Constants.RESPONSE_TIME_AVG_DEST);
    //metrics.add(Constants.RESPONSE_TIME_SUM_DEST);

    LineChartParams chartMins = new LineChartParams();
    //eg: line, timestamp, MINUTE:30, country=US:url=/home, sum:count:avg:avgRespTime
    String[] chartMinsInput = new String[] {CHART_TYPE.LINE.toString(), Constants.TIME_ATTR, TimeUnit.MINUTES + ":15", null, Constants.BYTES_SUM_DEST + ":" + Constants.COUNT_DEST};
    chartMins.init(chartMinsInput);

    LineChartParams chartHours = new LineChartParams();
    String[] chartHoursInput = new String[] {CHART_TYPE.LINE.toString(), Constants.TIME_ATTR, TimeUnit.HOURS + ":15", null, Constants.BYTES_SUM_DEST + ":" + Constants.COUNT_DEST};
    chartHours.init(chartHoursInput);

    LineChartParams chartMinsCountry = new LineChartParams();
    String[] chartMinsCountryInput = new String[] {CHART_TYPE.LINE.toString(), Constants.TIME_ATTR, TimeUnit.MINUTES + ":15", "geoip_country_name=US", Constants.BYTES_SUM_DEST + ":" + Constants.COUNT_DEST};
    chartMinsCountry.init(chartMinsCountryInput);

    LineChartParams[] lineChartParams = new LineChartParams[] {
      chartMins,
      chartHours,
      chartMinsCountry
    };

    return lineChartParams;
  }

  private ApacheLogParseMapOutputOperator getParserOperator(DAG dag)
  {
    ApacheLogParseMapOutputOperator parser = dag.addOperator("LogParser", new ApacheLogParseMapOutputOperator());
    parser.setRegexGroups(new String[] {null, "ip", null, "userid", Constants.TIME_ATTR, "url", "httpCode", "bytes", null, "agent"});
    parser.setLogRegex("^([\\d\\.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"[A-Z]+ (.+?) HTTP/\\S+\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\".*");
    GeoIPExtractor geoIPExtractor = new GeoIPExtractor();
    geoIPExtractor.setDatabasePath("/sfw/geolite/GeoLiteCity.dat");
    parser.registerInformationExtractor("ip", geoIPExtractor);
    parser.registerInformationExtractor("agent", new UserAgentExtractor());
    TimestampExtractor timestampExtractor = new TimestampExtractor();
    timestampExtractor.setDateFormatString("dd/MMM/yyyy:HH:mm:ss Z");
    parser.registerInformationExtractor(Constants.TIME_ATTR, timestampExtractor);
    return parser;
  }

  private void setLibraryJars(DAG dag)
  {
    List<Class<?>> containingJars = new ArrayList<Class<?>>();
    containingJars.add(com.maxmind.geoip.LookupService.class);
    containingJars.add(net.sf.uadetector.UserAgentStringParser.class);
    containingJars.add(net.sf.uadetector.service.UADetectorServiceFactory.class);
    containingJars.add(net.sf.qualitycheck.Check.class);
    containingJars.add(com.mongodb.DB.class);

    String oldlibjar = dag.getValue(DAGContext.LIBRARY_JARS);
    if (oldlibjar == null) {
      oldlibjar = "";
    }

    StringBuilder libjars = new StringBuilder(oldlibjar);
    for (Class<?> clazz : containingJars) {
      if (libjars.length() != 0) {
        libjars.append(",");
      }
      libjars.append(clazz.getProtectionDomain().getCodeSource().getLocation().toString());
    }
    dag.setAttribute(DAGContext.LIBRARY_JARS, libjars.toString());
  }

}
