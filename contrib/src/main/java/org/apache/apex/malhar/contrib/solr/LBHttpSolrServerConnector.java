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

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;

/**
 * Initializes LBHttpSolrServer instance of Solr Server.<br>
 * <br>
 * properties:<br>
 * solrServerUrls - Urls of solr Server
 * httpClient - http client instance
 * responseParser - ResponseParser instance
 *
 * @since 2.0.0
 */
public class LBHttpSolrServerConnector extends SolrServerConnector
{
  @NotNull
  private String[] solrServerUrls;
  private HttpClient httpClient;
  private ResponseParser responseParser;

  @Override
  public void connect() throws IOException
  {
    if (httpClient == null && responseParser == null) {
      solrServer = new LBHttpSolrServer(solrServerUrls);
    } else if (responseParser == null) {
      solrServer = new LBHttpSolrServer(httpClient, solrServerUrls);
    } else {
      solrServer = new LBHttpSolrServer(httpClient, responseParser, solrServerUrls);
    }
  }

  public void setSolrServerUrls(String[] solrServerUrls)
  {
    this.solrServerUrls = solrServerUrls;
  }

  /*
   * Urls of solr Server httpClient - http client instance responseParser - ResponseParser instance
   * Gets the solr server urls
   */
  public String[] getSolrServerUrls()
  {
    return solrServerUrls;
  }

  public void setHttpClient(HttpClient httpClient)
  {
    this.httpClient = httpClient;
  }

  /*
   * HttpClient instance
   * Gets the HTTP Client instance
   */
  public HttpClient getHttpClient()
  {
    return httpClient;
  }

  public void setResponseParser(ResponseParser responseParser)
  {
    this.responseParser = responseParser;
  }

  public ResponseParser getResponseParser()
  {
    return responseParser;
  }
}
