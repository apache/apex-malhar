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
package org.apache.apex.malhar.contrib.elasticsearch;

import java.util.Map;

import javax.validation.constraints.NotNull;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;

import com.datatorrent.api.Context.OperatorContext;

/**
 *
 * @since 2.1.0
 */
public abstract class ElasticSearchMapInputOperator<T extends Map<String, Object>> extends AbstractElasticSearchInputOperator<T, ElasticSearchConnectable>
{
  @NotNull
  protected String indexName;
  @NotNull
  protected String type;

  /**
   *
   */
  public ElasticSearchMapInputOperator()
  {
    this.store = new ElasticSearchConnectable();
  }

  /**
   * {@link SearchRequestBuilder} properties which do not change for each window are set during operator initialization.
   *
   * @see org.apache.apex.malhar.contrib.elasticsearch.AbstractElasticSearchInputOperator#setup(com.datatorrent.api.Context.OperatorContext)
   */
  @Override
  public void setup(OperatorContext t1)
  {
    super.setup(t1);
    searchRequestBuilder.setIndices(indexName).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.apex.malhar.contrib.elasticsearch.AbstractElasticSearchInputOperator#convertToTuple(org.elasticsearch.search
   * .SearchHit)
   */
  @Override
  protected T convertToTuple(SearchHit hit)
  {
    Map<String, Object> tuple = hit.getSource();
    return (T)tuple;
  }

  /**
   * @param indexName
   *          the indexName to set
   */
  public void setIndexName(String indexName)
  {
    this.indexName = indexName;
  }

  /**
   * @return the indexName
   */
  public String getIndexName()
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

  /**
   * @return the type
   */
  public String getType()
  {
    return type;
  }

}
