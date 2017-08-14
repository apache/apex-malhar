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
package org.apache.apex.malhar.contrib.apachelog;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.io.fs.TailFsInputOperator;
import org.apache.apex.malhar.lib.logs.ApacheLogParseMapOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;

/**
 * An implementation of Streaming Application that reads from a local apache log file on the fly and dumps the parsed data to output.
 * <p>
 * @displayName Application Local Log
 * @category Output
 * @tags reader, streaming
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
