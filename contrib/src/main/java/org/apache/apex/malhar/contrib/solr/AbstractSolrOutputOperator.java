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
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.db.AbstractStoreOutputOperator;
import org.apache.apex.malhar.lib.db.Connectable;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

import com.datatorrent.api.Context.OperatorContext;

/**
 * This is a base implementation of a Solr output operator, which adds data to Solr Server.&nbsp; Subclasses should
 * implement the methods convertTuple.
 * <p>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method convertTuple() <br>
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
 * @displayName Abstract Solr Output
 * @category Search Engine
 * @tags output operator
 *
 * @since 2.0.0
 */
public abstract class AbstractSolrOutputOperator<T, S extends Connectable> extends AbstractStoreOutputOperator<T, S>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractSolrOutputOperator.class);
  @NotNull
  protected SolrServerConnector solrServerConnector;
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private transient Queue<SolrInputDocument> docBuffer;

  @Override
  public void processTuple(T tuple)
  {
    if (docBuffer.size() >= bufferSize) {
      processTuples();
    }
    SolrInputDocument solrDocument = convertTuple(tuple);
    if (solrDocument != null) {
      docBuffer.add(solrDocument);
    }
  }

  /**
   * Converts the object into Solr document format
   *
   * @param object to be stored to Solr Server
   * @return
   */
  public abstract SolrInputDocument convertTuple(T tuple);

  @Override
  public void setup(OperatorContext context)
  {
    docBuffer = new ArrayBlockingQueue<SolrInputDocument>(bufferSize);
    try {
      solrServerConnector.connect();
    } catch (Exception ex) {
      throw new RuntimeException("Unable to connect to Solr server", ex);
    }
  }

  @Override
  public void teardown()
  {
    docBuffer.clear();
    solrServerConnector.getSolrServer().shutdown();
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    processTuples();
  }

  private void processTuples()
  {
    try {
      SolrServer solrServer = solrServerConnector.getSolrServer();
      solrServer.add(docBuffer);
      UpdateResponse response = solrServer.commit();
      if (response.getStatus() != 0) {
        throw new RuntimeException("Unable to add data to solr server");
      }
      logger.debug("Submitted documents batch of size " + docBuffer.size() + " to Solr server.");
      docBuffer.clear();
    } catch (SolrServerException ex) {
      throw new RuntimeException("Unable to insert documents during process", ex);
    } catch (SolrException ex) {
      throw new RuntimeException("Unable to insert documents during process", ex);
    } catch (IOException iox) {
      throw new RuntimeException("Unable to insert documents during process", iox);
    }
  }

  public int getBufferSize()
  {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }

  public SolrServerConnector getSolrServerConnector()
  {
    return solrServerConnector;
  }

  public void setSolrServerConnector(SolrServerConnector solrServerConnector)
  {
    this.solrServerConnector = solrServerConnector;
  }

}
