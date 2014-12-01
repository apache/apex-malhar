package com.datatorrent.contrib.solr;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.servlet.SolrRequestParsers;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

public class SolrInputOperatorTest extends SolrOperatorTest
{
  private AbstractSolrInputOperator<SolrDocument, SolrServerConnector> inputOperator;
  private CollectorTestSink testSink;

  @Before
  @Override
  public void setUp() throws Exception
  {
    super.setUp();
    SolrServerConnector solrServerConnector = new SolrServerConnector() {
      @Override
      public void connect()
      {
        this.solrServer = SolrInputOperatorTest.this.solrServer;
      }
    };
    inputOperator = new SolrInputOperator();
    inputOperator.setSolrServerConnector(solrServerConnector);
    inputOperator.setup(null);
    testSink = new CollectorTestSink();
    inputOperator.outputPort.setSink(testSink);
  }

  @Test
  public void testEmptyResult()
  {
    inputOperator.beginWindow(1);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    assertEquals("No documents in Solr Server", 0, testSink.collectedTuples.size());
  }

  @Test
  public void testInputOperator() throws Exception
  {
    addFilesToServer(inputOperator.getSolrServer());
    inputOperator.beginWindow(1);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    assertEquals("One document matches given query", 1, testSink.collectedTuples.size());
  }

  @Test(expected = RuntimeException.class)
  public void testServerUnavailable()
  {
    solrServer.shutdown();
    inputOperator.beginWindow(1);
    inputOperator.emitTuples();
    fail("Should throw an exception as server is unreachable.");
    inputOperator.endWindow();
  }

  @Test
  public void testLastEmittedTuple() throws Exception
  {
    addFilesToServer(inputOperator.getSolrServer());
    inputOperator.beginWindow(1);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    SolrDocument doc = inputOperator.getLastEmittedTuple();

    assertEquals("One document matches given query", "books.csv", doc.getFieldValue("name"));
  }

  private void addFilesToServer(SolrServer server) throws SolrServerException, IOException
  {
    List<SolrInputDocument> docs = getDocumentsForTest();
    server.add(docs);
    server.commit();
  }

  class SolrInputOperator extends AbstractSolrInputOperator<SolrDocument, SolrServerConnector>
  {

    @Override
    public SolrParams getQueryParams()
    {
      SolrParams solrParams = SolrRequestParsers.parseQueryString("q=Kings&sort=name desc");
      return solrParams;
    }

    @Override
    protected void emitTuple(SolrDocument document)
    {
      outputPort.emit(document);
    }

  }
}
