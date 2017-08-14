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
package org.apache.apex.malhar.contrib.solr;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.apache.solr.client.solrj.impl.CloudSolrServer;

/**
 * Initializes CloudServer instance of Solr Server.<br>
 * <br>
 * Properties:<br>
 * zookeeperHost - The client endpoint of the zookeeper quorum containing the cloud state, in the form HOST:PORT<br>
 * updateToLeader - sends updates only to leaders - defaults to true
 *
 * @since 2.0.0
 */
public class CloudSolrServerConnector extends SolrServerConnector
{

  @NotNull
  private String zookeeperHost;
  private boolean updateToLeader;

  @Override
  public void connect() throws IOException
  {
    solrServer = new CloudSolrServer(zookeeperHost, updateToLeader);
  }

  public void setSolrZookeeperHost(String solrServerURL)
  {
    this.zookeeperHost = solrServerURL;
  }

  /*
   * The client endpoint of the zookeeper quorum containing the cloud state, in the form HOST:PORT
   * Gets the zookeeper host
   */
  public String getSolrZookeeperHost()
  {
    return zookeeperHost;
  }

  public void setUpdateToLeader(boolean updateToLeader)
  {
    this.updateToLeader = updateToLeader;
  }

  /*
   * Sends updates only to leaders - defaults to true
   * Gets boolean value of updateToLeader
   */
  public boolean getUpdateToLeader()
  {
    return updateToLeader;
  }

}
