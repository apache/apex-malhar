package com.datatorrent.contrib.solr;

import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.servlet.SolrRequestParsers;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

public class SolrOutputOperatorTest extends SolrOperatorTest
{
  private AbstractSolrOutputOperator<SolrInputDocument, SolrServerConnector> outputOperator;

  @Override
  @Before
  public void setUp() throws Exception
  {
    super.setUp();
    SolrServerConnector solrServerConnector = new SolrServerConnector() {
      @Override
      public void connect()
      {
        this.solrServer = SolrOutputOperatorTest.this.solrServer;
      }
    };
    outputOperator = new SolrOutputOperator();
    outputOperator.setSolrServerConnector(solrServerConnector);
    outputOperator.setup(null);
  }

  @Test(expected = RuntimeException.class)
  public void testServerUnavailable()
  {
    solrServer.shutdown();

    SolrInputDocument doc = new SolrInputDocument();
    outputOperator.setup(new OperatorContextTestHelper.TestIdOperatorContext(111));
    outputOperator.beginWindow(1L);
    outputOperator.input.process(doc);
    outputOperator.endWindow();
    fail("Should throw an exception as server is unreachable.");
  }

  @Test
  public void testOutputOperator() throws Exception
  {
    List<SolrInputDocument> docs = getDocumentsForTest();

    outputOperator.setup(new OperatorContextTestHelper.TestIdOperatorContext(111));
    outputOperator.beginWindow(1L);
    for (SolrInputDocument doc : docs) {
      outputOperator.input.process(doc);
    }
    outputOperator.endWindow();

    QueryResponse queryResponse = solrServer.query(SolrRequestParsers.parseQueryString("q=*"));
    assertEquals("Mismatch in documents count in Solr Server", docs.size(), queryResponse.getResults().size());
  }

  class SolrOutputOperator extends AbstractSolrOutputOperator<SolrInputDocument, SolrServerConnector>
  {

    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
    }

    @Override
    public SolrInputDocument convertTuple(SolrInputDocument tuple)
    {
      return tuple;
    }
  }

}
