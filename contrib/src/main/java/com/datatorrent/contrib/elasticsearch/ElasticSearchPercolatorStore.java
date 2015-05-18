/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.elasticsearch;

import java.io.IOException;

import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;

import com.datatorrent.common.util.DTThrowable;

/**
 * 
 * @since 2.1.0
 */
public class ElasticSearchPercolatorStore extends ElasticSearchConnectable
{
  private static final String PERCOLATOR_TYPE = ".percolator";

  ElasticSearchPercolatorStore(String hostName, int port)
  {
    setHostName(hostName);
    setPort(port);
  }

  public void registerPercolateQuery(String indexName, String queryName, QueryBuilder queryBuilder)
  {
    try {
      
      client.prepareIndex(indexName, PERCOLATOR_TYPE, queryName)
        .setSource(XContentFactory.jsonBuilder()
            .startObject()
            .field("query", queryBuilder)
            .endObject())
        .setRefresh(true)
        .execute().actionGet();
      
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }
  
  public PercolateResponse percolate(String[] indexNames, String documentType, Object tuple){
    XContentBuilder docBuilder;
    try {
      
      docBuilder = XContentFactory.jsonBuilder().startObject();
      docBuilder.field("doc").startObject(); //This is needed to designate the document
      docBuilder.field("content", tuple);
      docBuilder.endObject(); //End of the doc field
      docBuilder.endObject();//End of the JSON root object
      
      return client.preparePercolate().setIndices(indexNames)
          .setDocumentType(documentType)
          .setSource(docBuilder)
          .execute().actionGet();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    return null;
  }
}
