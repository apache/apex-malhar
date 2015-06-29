/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
