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

import javax.validation.constraints.NotNull;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

/**
 * Initializes HttpSolrServer instance of Solr Server.<br>
 * <br>
 * properties:<br>
 * baseURL - The URL of the Solr server.
 *
 * @since 2.0.0
 */
public class HttpSolrServerConnector extends SolrServerConnector
{
  @NotNull
  private String solrServerURL;

  @Override
  public void connect()
  {
    solrServer = new HttpSolrServer(solrServerURL);
  }

  public void setSolrServerURL(String solrServerURL)
  {
    this.solrServerURL = solrServerURL;
  }

  /*
   * The URL of the Solr server.
   * Gets the URL of solr server
   */
  public String getSolrServerURL()
  {
    return solrServerURL;
  }

}
