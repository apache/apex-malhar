/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datatorrent.contrib.apachelog;

import com.datatorrent.api.*;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.logs.ApacheLogParseMapOutputOperator;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 * An implementation of Streaming Application that generates apache log file on the fly and dumps the parsed data to output.
 * 
 * <p>
 * @displayName: Application Log Generator
 * @category: db
 * @tag: streaming
 * @since 0.9.4
 */
public class ApplicationLogGenerator implements StreamingApplication
{
  private void setLibraryJars(DAG dag)
  {
    List<Class<?>> containingJars = new ArrayList<Class<?>>();
    containingJars.add(com.maxmind.geoip.LookupService.class);
    containingJars.add(net.sf.uadetector.UserAgentStringParser.class);
    containingJars.add(net.sf.uadetector.service.UADetectorServiceFactory.class);
    containingJars.add(net.sf.qualitycheck.Check.class);

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

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    setLibraryJars(dag);
    ApacheLogInputGenerator log = dag.addOperator("log", new ApacheLogInputGenerator());
    log.setIpAddressFile("/com/datatorrent/contrib/apachelog/ipaddress.txt");
    log.setUrlFile("/com/datatorrent/contrib/apachelog/urls.txt");
    log.setAgentFile("/com/datatorrent/contrib/apachelog/agents.txt");
    log.setRefererFile("/com/datatorrent/contrib/apachelog/referers.txt");

    ApacheLogParseMapOutputOperator parse = dag.addOperator("parse", new ApacheLogParseMapOutputOperator());
    GeoIPExtractor geoIPExtractor = new GeoIPExtractor();

    // Can't put this file in resources until licensing issue is straightened out
    geoIPExtractor.setDatabasePath("/home/david/GeoLiteCity.dat");

    parse.registerInformationExtractor("ip", geoIPExtractor);
    parse.registerInformationExtractor("agent", new UserAgentExtractor());
    TimestampExtractor timestampExtractor = new TimestampExtractor();
    timestampExtractor.setDateFormatString("dd/MMM/yyyy:HH:mm:ss Z");
    parse.registerInformationExtractor("time", timestampExtractor);

    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());

    dag.addStream("log-parse", log.output, parse.data);
    dag.addStream("parse-console", parse.output, console.input).setLocality(Locality.CONTAINER_LOCAL);

  }

}
