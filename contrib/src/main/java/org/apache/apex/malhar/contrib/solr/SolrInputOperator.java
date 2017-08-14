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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.servlet.SolrRequestParsers;

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
