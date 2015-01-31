package com.datatorrent.contrib.solr;

import java.io.Closeable;
import java.io.IOException;

import org.apache.solr.client.solrj.SolrServer;

import com.datatorrent.lib.db.Connectable;

/**
 * SolrServerConnector
 *
 * @since 2.0.0
 */
public abstract class SolrServerConnector implements Connectable, Closeable
{
  protected SolrServer solrServer;

  public SolrServer getSolrServer()
  {
    return solrServer;
  }

  public void setSolrServer(SolrServer solrServer)
  {
    this.solrServer = solrServer;
  }

  @Override
  public void disconnect() throws IOException
  {
    if (solrServer != null) {
      solrServer.shutdown();
    }
  }

  @Override
  public boolean isConnected()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException
  {
    if (solrServer != null) {
      solrServer.shutdown();
    }
  }

}
