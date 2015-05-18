/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.*/

package com.datatorrent.contrib.elasticsearch;

import java.util.Map;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concrete implementation of {@link AbstractElasticSearchOutputOperator} demonstrating the functionality for Tuples of
 * Map type.
 *
 * @since 2.1.0
 */
public class ElasticSearchMapOutputOperator<T extends Map<String, Object>> extends AbstractElasticSearchOutputOperator<T, ElasticSearchConnectable>
{
  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchMapOutputOperator.class);
  private String idField;
  private String indexName;
  private String type;

  /**
   * 
   */
  public ElasticSearchMapOutputOperator()
  {
    this.store = new ElasticSearchConnectable();
  }


  /*
   * (non-Javadoc)
   * 
   * @see
   * com.datatorrent.contrib.elasticsearch.AbstractElasticSearchOutputOperator#setSource(org.elasticsearch.action.index
   * .IndexRequestBuilder, java.lang.Object)
   */
  @Override
  protected IndexRequestBuilder setSource(IndexRequestBuilder indexRequestBuilder, T tuple)
  {
    return indexRequestBuilder.setSource(tuple);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.datatorrent.contrib.elasticsearch.AbstractElasticSearchOutputOperator#getId(java.lang.Object)
   */
  @Override
  protected String getId(T tuple)
  {
    if (idField == null) {
      return null;
    } else {
      return tuple.get(idField).toString();
    }

  }

  /**
   * @param idField
   *          the idField to set
   */
  public void setIdField(String idField)
  {
    this.idField = idField;
  }

  /**
   * @return the idField
   */
  public String getIdField()
  {
    return idField;
  }

  /**
   * @param indexName
   *          the indexName to set
   */
  public void setIndexName(String indexName)
  {
    this.indexName = indexName;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.datatorrent.contrib.elasticsearch.AbstractElasticSearchOutputOperator#getIndexName(java.lang.Object)
   */
  @Override
  protected String getIndexName(T tuple)
  {
    return indexName;
  }

  /**
   * @param type
   *          the type to set
   */
  public void setType(String type)
  {
    this.type = type;
  }

  /* (non-Javadoc)
   * @see com.datatorrent.contrib.elasticsearch.AbstractElasticSearchOutputOperator#getType(java.lang.Object)
   */
  @Override
  protected String getType(T tuple)
  {
    return type;
  }  
}
