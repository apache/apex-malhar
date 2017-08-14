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
package org.apache.apex.malhar.contrib.nifi.demo;

import org.apache.apex.malhar.contrib.nifi.NiFiSinglePortInputOperator;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

/**
 * A sample application that shows how to receive data to a NiFi Output Port.
 */
public class TestNiFiInputApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    final SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
        .url("http://localhost:8080/nifi")
        .portName("Apex")
        .requestBatchCount(5)
        .buildConfig();

    final SiteToSiteClient.Builder builder = new SiteToSiteClient.Builder().fromConfig(clientConfig);

    final WindowDataManager windowDataManager = new WindowDataManager.NoopWindowDataManager();

    NiFiSinglePortInputOperator nifi = dag.addOperator("nifi", new NiFiSinglePortInputOperator(builder, windowDataManager));
    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("nifi_console", nifi.outputPort, console.input).setLocality(null);
  }

  public static void main(String[] args) throws Exception
  {
    StreamingApplication app = new TestNiFiInputApplication();
    LocalMode.runApp(app, new Configuration(false), 10000);
    Thread.sleep(2000);
    System.exit(0);
  }
}
