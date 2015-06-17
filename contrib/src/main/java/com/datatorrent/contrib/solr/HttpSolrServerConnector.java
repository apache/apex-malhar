package com.datatorrent.contrib.solr;

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
