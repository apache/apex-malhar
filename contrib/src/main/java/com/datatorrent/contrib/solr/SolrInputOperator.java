package com.datatorrent.contrib.solr;

import java.util.Map;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.servlet.SolrRequestParsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Implementation of AbstractSolrInputOperator. Reads query from properties file <br>
 *
 * @since 2.0.0
 */
public class SolrInputOperator extends AbstractSolrInputOperator<Map<String, Object>, SolrServerConnector>
{
  private static final Logger logger = LoggerFactory.getLogger(SolrInputOperator.class);
  private String solrQuery;

  @Override
  protected void emitTuple(SolrDocument document)
  {
    outputPort.emit(document.getFieldValueMap());
  }

  @Override
  public SolrParams getQueryParams()
  {
    SolrParams solrParams;
    if (solrQuery != null) {
      solrParams = SolrRequestParsers.parseQueryString(solrQuery);
    } else {
      logger.debug("Solr document fetch query is not set, using wild card query for search.");
      solrParams = SolrRequestParsers.parseQueryString("*");
    }
    return solrParams;
  }

  /*
   * Solr search query
   * Gets the solr search query
   */
  public String getSolrQuery()
  {
    return solrQuery;
  }

  public void setSolrQuery(String solrQuery)
  {
    this.solrQuery = solrQuery;
  }

}
