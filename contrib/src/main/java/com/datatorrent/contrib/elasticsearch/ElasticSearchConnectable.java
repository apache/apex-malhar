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
package com.datatorrent.contrib.elasticsearch;

import java.io.IOException;
import java.net.InetAddress;

import javax.validation.constraints.NotNull;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.datatorrent.lib.db.Connectable;

/**
 * Elastic search base connector which has basic information for an operator <br>
 * Properties:<br>
 * <b>hostname</b>:the host name of the elastic search cluster node to connect to, not null<br>
 * <b>port</b>:port number of the elastic search cluster node to connect to, not null<br>
 * <b>client</b>:created when connected to elastic search cluster node <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * hostName batchSize <br>
 * <b>data type:</br>the insertion data can support all the Objects mongoDB supports<br>
 *
 * <b>Benchmarks</b>: <br>
 *
 * @since 2.1.0
 * */

public class ElasticSearchConnectable implements Connectable
{
  @NotNull
  protected String hostName;
  @NotNull
  protected int port;

  @NotNull
  protected String clusterName;

  protected transient TransportClient client;

  /**
   * @return the hostname
   */
  public String getHostName()
  {
    return hostName;
  }

  /**
   * @param hostname
   *          the hostname to set
   */
  public void setHostName(String hostname)
  {
    this.hostName = hostname;
  }

  /**
   * @return the port
   */
  public int getPort()
  {
    return port;
  }

  /**
   * @param port
   *          the port to set
   */
  public void setPort(int port)
  {
    this.port = port;
  }

  /**
   *
   * @return
   */
  public String getClusterName() 
  {
    return clusterName;
  }

  /**
   *
   * @param clusterName
   */
  public void setClusterName(String clusterName) 
  {
    this.clusterName = clusterName;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.datatorrent.lib.db.Connectable#connect()
   */
  @Override
  public void connect() throws IOException
  {
    Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.sniff", true).build();
    client = TransportClient.builder().settings(settings).build();
    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName),port));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.datatorrent.lib.db.Connectable#disconnect()
   */
  @Override
  public void disconnect() throws IOException
  {
    if (client != null) {
      client.close();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.datatorrent.lib.db.Connectable#isConnected()
   */
  @Override
  public boolean isConnected()
  {
    if (client != null) {
      return client.connectedNodes().size() != 0;
    }
    return false;
  }

}
