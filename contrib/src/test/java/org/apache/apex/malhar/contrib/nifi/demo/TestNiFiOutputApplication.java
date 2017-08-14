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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.apex.malhar.contrib.nifi.NiFiDataPacket;
import org.apache.apex.malhar.contrib.nifi.NiFiDataPacketBuilder;
import org.apache.apex.malhar.contrib.nifi.NiFiSinglePortOutputOperator;
import org.apache.apex.malhar.contrib.nifi.StandardNiFiDataPacket;
import org.apache.apex.malhar.lib.testbench.RandomEventGenerator;
import org.apache.apex.malhar.lib.wal.WindowDataManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

/**
 * A sample application that shows how to send data to a NiFi Input Port.
 */
public class TestNiFiOutputApplication implements StreamingApplication
{

  /**
   * A builder that can create a NiFiDataPacket from a string.
   */
  public static class StringNiFiDataPacketBuilder implements NiFiDataPacketBuilder<String>
  {
    @Override
    public NiFiDataPacket createNiFiDataPacket(String s)
    {
      return new StandardNiFiDataPacket(s.getBytes(StandardCharsets.UTF_8), new HashMap<String, String>());
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    final SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
        .url("http://localhost:8080/nifi")
        .portName("Apex")
        .buildConfig();

    final int batchSize = 1;
    final SiteToSiteClient.Builder builder = new SiteToSiteClient.Builder().fromConfig(clientConfig);
    final NiFiDataPacketBuilder<String> dataPacketBuilder = new StringNiFiDataPacketBuilder();
    final WindowDataManager windowDataManager = new WindowDataManager.NoopWindowDataManager();

    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());

    NiFiSinglePortOutputOperator nifi = dag.addOperator("nifi",
        new NiFiSinglePortOutputOperator(builder, dataPacketBuilder, windowDataManager, batchSize));

    dag.addStream("rand_nifi", rand.string_data, nifi.inputPort).setLocality(null);
  }

  public static void main(String[] args) throws Exception
  {
    StreamingApplication app = new TestNiFiOutputApplication();
    LocalMode.runApp(app, new Configuration(false), 10000);
    Thread.sleep(2000);
    System.exit(0);
  }

}
