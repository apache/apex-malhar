package com.datatorrent.contrib.solr;

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
