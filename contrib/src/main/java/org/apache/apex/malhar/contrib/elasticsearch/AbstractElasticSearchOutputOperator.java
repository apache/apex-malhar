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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import javax.validation.constraints.Min;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;

import org.apache.apex.malhar.lib.db.AbstractStoreOutputOperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This is the base implementation for a non-transactional batch output operator for ElasticSearch.
 *
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port <br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>batchSize</b>:size for each batch insert, default value is 1000<br>
 *
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * hostName<br>
 * port<br>
 * batchSize <br>
 *
 * <b>Benchmarks</b>: <br>
 * </p>
 *
 * @displayName Elastic Search Output
 * @category Output
 * @tags elastic search
 *
 * @since 2.1.0
 */
public abstract class AbstractElasticSearchOutputOperator<T, S extends ElasticSearchConnectable> extends AbstractStoreOutputOperator<T, S>
{
  protected static final int DEFAULT_BATCH_SIZE = 1000;
  @Min(1)
  protected int batchSize = DEFAULT_BATCH_SIZE;
  protected transient Queue<T> tupleBatch;

  /**
   * Initialize transient fields such as {@code tupleBatch}
   *
   * @see org.apache.apex.malhar.lib.db.AbstractStoreOutputOperator#setup(com.datatorrent.api.Context.OperatorContext)
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    tupleBatch = new ArrayBlockingQueue<T>(batchSize);
  }

  /**
   * Adds tuple to the queue.
   * Calls {@link #processBatch()} if queue is full
   *
   * @see org.apache.apex.malhar.lib.db.AbstractStoreOutputOperator#processTuple(java.lang.Object)
   */
  public void processTuple(T tuple)
  {
    tupleBatch.add(tuple);
    if (tupleBatch.size() >= batchSize) {
      processBatch();
    }
  }

  /**
   * Flush the batch queue at end window
   */
  @Override
  public void endWindow()
  {
    super.endWindow();
    processBatch();
  }

  /**
   * This will flush all the tuples from queue to ElasticSearch.
   * It uses BulkRequestBuilder API for sending batch.
   */
  private void processBatch()
  {
    BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(store.client);
    while (!tupleBatch.isEmpty()) {
      T tuple = tupleBatch.remove();
      IndexRequestBuilder indexRequestBuilder = getIndexRequestBuilder(tuple);
      bulkRequestBuilder.add(indexRequestBuilder);
    }
    BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      DTThrowable.rethrow(new Exception(bulkResponse.buildFailureMessage()));
    }
  }

  /**
   * Create {@link IndexRequestBuilder} for this tuple.
   * It calls {@link #getId(T)}, {@link #getIndexName(T)}, {@link #getType(T)}.
   *
   * @param tuple
   * @return
   */
  protected IndexRequestBuilder getIndexRequestBuilder(T tuple)
  {
    IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(store.client, getIndexName(tuple));
    String id = getId(tuple);
    if (id != null) {
      indexRequestBuilder.setId(id);
    }
    indexRequestBuilder.setType(getType(tuple));
    return setSource(indexRequestBuilder, tuple);
  }

  /**
   * User actions required for format conversions from tuple to one of the source types supported by IndexRequest.
   * @param tuple
   * @return
   */
  protected abstract IndexRequestBuilder setSource(IndexRequestBuilder indexRequestBuilder,T tuple);

  /**
   * Determine id for the given tuple.<br>
   * If tuples do not have any field mapping to unique id then this function may return null. In this case
   * ElasticSearch will add auto-generated _id field to the document.
   * ElasticSearch will treat every tuple as fresh record.
   * If user requires {@code ProcessingMode.EXACTLY_ONCE} support; then user must
   * implement {@link #getId(Object)} and make sure that distinct value is returned for each record.
   *
   * @param tuple
   * @return
   */
  protected abstract String getId(T tuple);

  /**
   * Decides indexName under which this tuple gets stored.
   * Depending on scenario, user may decide index all tuples under same indexName
   * (In this case user can return constant value for indexName).
   * Or user may have custom logic for deriving indexName
   * @param tuple
   * @return
   */
  protected abstract String getIndexName(T tuple);

  /**
   * Decides type for given tuple.
   * Depending on scenario, user may decide index all tuples under same type
   * (In this case user can return constant value for type).
   * Or user may have custom logic for deriving type.
   * @param tuple
   * @return
   */
  protected abstract String getType(T tuple);

    /**
     * @return the batchSize
     */
  public int getBatchSize()
  {
    return batchSize;
  }

    /**
     * @param batchSize the batchSize to set
     */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

}
