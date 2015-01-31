package com.datatorrent.contrib.solr;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import javax.validation.constraints.NotNull;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;

/**
 * Initializes ConcurrentUpdateServer instance of Solr Server.<br>
 * <br>
 * properties:<br>
 * solrServerURL - The Solr server URL<br>
 * queueSize - The buffer size before the documents are sent to the server <br>
 * threadCount - The number of background threads used to empty the queue<br>
 * httpClient - HttpClient instance
 *
 * @since 2.0.0
 */
public class ConcurrentUpdateSolrServerConnector extends SolrServerConnector
{
  private static final int DEFAULT_THREAD_COUNT = 5;
  private static final int DEFAULT_QUEUE_SIZE = 1024;
  @NotNull
  private String solrServerURL;
  private int queueSize = DEFAULT_QUEUE_SIZE;
  private int threadCount = DEFAULT_THREAD_COUNT;
  private HttpClient httpClient;
  private ExecutorService executorService;
  private boolean streamDeletes = false;

  @Override
  public void connect() throws IOException
  {
    if (httpClient == null && executorService == null) {
      solrServer = new ConcurrentUpdateSolrServer(solrServerURL, queueSize, threadCount);
    } else if (executorService == null) {
      solrServer = new ConcurrentUpdateSolrServer(solrServerURL, httpClient, queueSize, threadCount);
    } else {
      solrServer = new ConcurrentUpdateSolrServer(solrServerURL, httpClient, queueSize, threadCount, executorService, streamDeletes);
    }

  }

  // set this property in dt-site.xml
  public void setSolrServerURL(String solrServerURL)
  {
    this.solrServerURL = solrServerURL;
  }

  public String getSolrServerURL()
  {
    return solrServerURL;
  }

  // set this property in dt-site.xml
  public void setQueueSize(int queueSize)
  {
    this.queueSize = queueSize;
  }

  public int getQueueSize()
  {
    return queueSize;
  }

  // set this property in dt-site.xml
  public void setThreadCount(int threadCount)
  {
    this.threadCount = threadCount;
  }

  public int getThreadCount()
  {
    return threadCount;
  }

  public void setHttpClient(HttpClient httpClient)
  {
    this.httpClient = httpClient;
  }

  public HttpClient getHttpClient()
  {
    return httpClient;
  }

  // set this property in dt-site.xml
  public void setStreamDeletes(boolean streamDeletes)
  {
    this.streamDeletes = streamDeletes;
  }

  public boolean getStreamDeletes()
  {
    return streamDeletes;
  }

}
