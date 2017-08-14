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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.db.AbstractStoreInputOperator;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;

import com.datatorrent.api.Context.OperatorContext;

/**
 * This is a base implementation of a Solr input operator, which consumes data from solr search operations.&nbsp;
 * Subclasses should implement the methods getQueryParams, emitTuple for emitting tuples to downstream operators.
 * <p>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method getQueryParams() and emitTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 *
 * Shipped jars with this operator:<br>
 * <b>org.apache.solr.client.solrj.SolrServer.class</b> Solrj - Solr Java Client <br>
 * <br>
 * </p>
 *
 * @displayName Abstract Solr Input
 * @category Search Engine
 * @tags input operator
 *
 * @since 2.0.0
 */

public abstract class AbstractSolrInputOperator<T, S extends SolrServerConnector> extends AbstractStoreInputOperator<T, S>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractSolrInputOperator.class);
  @NotNull
  protected S solrServerConnector;
  private SolrDocument lastEmittedTuple;
  private long lastEmittedTimeStamp;

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      solrServerConnector.connect();
    } catch (IOException ex) {
      throw new RuntimeException("Unable to connect to Solr Server", ex);
    }
  }

  @Override
  public void teardown()
  {
    solrServerConnector.getSolrServer().shutdown();
  }

  public long getLastEmittedTimeStamp()
  {
    return lastEmittedTimeStamp;
  }

  public SolrDocument getLastEmittedTuple()
  {
    return lastEmittedTuple;
  }

  public SolrServer getSolrServer()
  {
    return solrServerConnector.getSolrServer();
  }

  public SolrServerConnector getSolrServerConnector()
  {
    return solrServerConnector;
  }

  public void setSolrServerConnector(S solrServerConnector)
  {
    this.solrServerConnector = solrServerConnector;
  }

  @Override
  public void emitTuples()
  {
    SolrParams solrQueryParams = getQueryParams();
    try {
      SolrServer solrServer = solrServerConnector.getSolrServer();
      QueryResponse response = solrServer.query(solrQueryParams);
      SolrDocumentList queriedDocuments = response.getResults();
      for (SolrDocument solrDocument : queriedDocuments) {
        emitTuple(solrDocument);
        lastEmittedTuple = solrDocument;
        lastEmittedTimeStamp = System.currentTimeMillis();

        logger.debug("Emiting document: " + solrDocument.getFieldValue("name"));
      }
    } catch (SolrServerException ex) {
      throw new RuntimeException("Unable to fetch documents from Solr server", ex);
    }
  }

  /**
   * Initialize SolrServer object
   */

  protected abstract void emitTuple(SolrDocument document);

  /**
   * Any concrete class has to override this method to return the query string which will be used to retrieve documents
   * from Solr server.
   *
   * @return Query string
   */
  public abstract SolrParams getQueryParams();

}
