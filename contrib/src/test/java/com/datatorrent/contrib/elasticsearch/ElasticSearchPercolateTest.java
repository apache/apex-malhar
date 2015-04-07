/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateResponse.Match;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Test class for percolate operator
 */
public class ElasticSearchPercolateTest
{
  private static final String INDEX_NAME = "mails";
  private static final String DOCUMENT_TYPE = "emailText";
  private static final String HOST_NAME = "localhost";
  private static final int PORT = 9300;

  private static final String GITHUB_TOPIC = "onGithub";
  private static final String MALHAR_TOPIC = "onMalhar";

  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchPercolateTest.class);

  private ElasticSearchPercolatorStore store;

  @Before
  public void setup() throws IOException
  {
    store = new ElasticSearchPercolatorStore(HOST_NAME, PORT);
    store.connect();
  }

  @Test
  public void testPercolate() throws IOException
  {
    try{
      registerPercolateQueries();
      checkPercolateResponse();
    }
    catch(NoNodeAvailableException e){
      //This indicates that elasticsearch is not running on a particular machine.
      //Silently ignore in this case.
    }
  }

  /**
   * Register percolate queries on ElasticSearch
   * 
   * @throws IOException
   * 
   */
  private void registerPercolateQueries() throws IOException
  {
    store.registerPercolateQuery(INDEX_NAME, GITHUB_TOPIC, new TermQueryBuilder("content", "github"));
    store.registerPercolateQuery(INDEX_NAME, MALHAR_TOPIC, new TermQueryBuilder("content", "malhar"));

  }

  /**
   * 
   */
  private void checkPercolateResponse()
  {
    ElasticSearchPercolatorOperator oper = new ElasticSearchPercolatorOperator();
    oper.hostName = HOST_NAME;
    oper.port = PORT;
    oper.indexName = INDEX_NAME;
    oper.documentType = DOCUMENT_TYPE;
    oper.setup(null);

    String[] messages = { "{content:'This will match only with malhar'}",

    "{content:'This will match only with github'}",

    "{content:'This will match with both github and malhar'}",

    "{content:'This will not match with any of them'}"

    };

    String[][] matches = {

    { MALHAR_TOPIC },

    { GITHUB_TOPIC },

    { GITHUB_TOPIC, MALHAR_TOPIC },

    {}

    };

    CollectorTestSink<PercolateResponse> sink = new CollectorTestSink<PercolateResponse>();
    oper.outputPort.setSink((CollectorTestSink) sink);

    for (String message : messages) {
      oper.inputPort.process(message);
    }

    int i = 0;
    for (PercolateResponse response : sink.collectedTuples) {
      List<String> matchIds = new ArrayList<String>();
      for (Match match : response.getMatches()) {
        matchIds.add(match.getId().toString());
      }
      Collections.sort(matchIds);
      
      Assert.assertArrayEquals(matchIds.toArray(), matches[i]);
      i++;
    }
  }

  @After
  public void cleanup() throws IOException
  {
    try{
      DeleteIndexResponse delete = store.client.admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
      if (!delete.isAcknowledged()) {
        logger.error("Index wasn't deleted");
      }

      store.disconnect();
    }
    catch(NoNodeAvailableException e){
      // Silently ignore if elasticsearch is not running.
    }
    
  }
}
