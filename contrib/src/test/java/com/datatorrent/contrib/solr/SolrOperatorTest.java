package com.datatorrent.contrib.solr;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.After;
import org.junit.Before;

public abstract class SolrOperatorTest extends AbstractSolrTestCase
{
  private static String SOLR_HOME;
  protected SolrServer solrServer;

  static {
    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
  }

  @Override
  @Before
  public void setUp() throws Exception
  {
    super.setUp();
    SOLR_HOME = getClass().getResource("/com/datatorrent/contrib/solr").getPath();

    initCore(getSolrConfigFile(), getSchemaFile(), getSolrHome());
    solrServer = new EmbeddedSolrServer(h.getCoreContainer(), h.getCore().getName());
  }

  @After
  public void afterTest()
  {
    h.close();
    solrServer.shutdown();
  }

  @Override
  public String getSolrHome()
  {
    return SOLR_HOME;
  }

  public static String getSolrConfigFile()
  {
    return SOLR_HOME + "/collection1/conf/solrconfig.xml";
  }

  public static String getSchemaFile()
  {
    return SOLR_HOME + "/collection1/conf/schema.xml";
  }

  List<SolrInputDocument> getDocumentsForTest() throws FileNotFoundException
  {
    File documentsFolder = new File(getSolrHome() + "/docs");
    if (!documentsFolder.isDirectory()) {
      return Collections.EMPTY_LIST;
    }

    List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (File document : documentsFolder.listFiles()) {
      SolrInputDocument solrDoc = new SolrInputDocument();
      String solrDocName = document.getName();
      solrDoc.addField("id", solrDocName.hashCode());
      solrDoc.addField("name", solrDocName);
      solrDoc.addField("text", new Scanner(document).useDelimiter("\\Z").next());
      docs.add(solrDoc);
    }
    return docs;
  }

}
