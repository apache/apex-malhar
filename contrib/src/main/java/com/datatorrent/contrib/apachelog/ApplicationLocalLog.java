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

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.fs.TailFsInputOperator;
import com.datatorrent.lib.logs.ApacheLogParseMapOutputOperator;
import java.text.SimpleDateFormat;
import org.apache.hadoop.conf.Configuration;

/**
 * This application just reads from a local apache log file on the fly and dumps the parsed data to output
 *
 * @since 0.9.4
 */
public class ApplicationLocalLog implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    TailFsInputOperator log = dag.addOperator("log", new TailFsInputOperator());
    log.setDelimiter('\n');
    log.setFilePath("/var/log/apache2/access.log");

    ApacheLogParseMapOutputOperator parse = dag.addOperator("parse", new ApacheLogParseMapOutputOperator());
    GeoIPExtractor geoIPExtractor = new GeoIPExtractor();
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
