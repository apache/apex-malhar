/*
 * Copyright (c) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.appdata.query;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context.OperatorContext;

/**
 * The QueryManagerSynchronous is a simple container for a {@link QueryExecutor} and a {@link QueueManager}.
 * It encapsulates the functionality of enqueueing a query and returning a result synchronously.
 * <br/>
 * <br/>
 * <b>Note:</b> Use {@link #newInstance} to create an instance of query processor.
 * It reduces the boilerplate code with respect to generics.
 *
 * @param <QUERY_TYPE> The type of the query.
 * @param <META_QUERY> The type of any query meta data.
 * @param <QUEUE_CONTEXT> The type of any context information used by the queue.
 * @param <RESULT> The type of the result returned by the {@link QueryExecutor}.
 */
public class QueryManagerSynchronous<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> implements Component<OperatorContext>
{
  /**
   * The {@link QueryExecutor} used to execute queries.
   */
  private QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryExecutor;
  /**
   * The {@link QueueManager} used to queue queries.
   */
  private QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager;

  /**
   *
   * @param queryComputer
   */
  private QueryManagerSynchronous(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryComputer)
  {
    setQueryExecutor(queryComputer);
    queryQueueManager = new SimpleQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>();
  }

  private QueryManagerSynchronous(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryComputer,
                                  QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager)
  {
    setQueryExecutor(queryComputer);
    setQueryQueueManager(queryQueueManager);
  }

  /**
   * A helper method to set a {@link QueryExecutor}.
   * @param queryExecutor The {@link QueryExecutor} to set on the {@link QueryManagerSynchronous}.
   */
  private void setQueryExecutor(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryExecutor)
  {
    this.queryExecutor = Preconditions.checkNotNull(queryExecutor);
  }

  /**
   * A helper method to set a {@link QueueManager}.
   * @param queryQueueManager The {@link QueueManager} to set on the {@link QueryManagerSynchronous}.
   */
  private void setQueryQueueManager(QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager)
  {
    this.queryQueueManager = Preconditions.checkNotNull(queryQueueManager);
  }

  /**
   * This method enqueues the given query with the corresponding meta data.
   * @param query The query to enqueue .
   * @param metaQuery Any additional query meta data required to execute the query.
   * @param queueContext The queue context information required to queue the query.
   * @return True if the query was successfully enqueued and false otherwise.
   */
  public boolean enqueue(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT queueContext)
  {
    return queryQueueManager.enqueue(query, metaQuery, queueContext);
  }

  /**
   * This method returns a result for the next query in the queue for which there is a result. If
   * there are no more queries with results in the queue, then this method will return null.
   * @return The result for the next query in the queue for which there is a result.
   */
  public RESULT process()
  {
    RESULT result = null;

    do {
      QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryBundle = queryQueueManager.dequeue();

      if(queryBundle == null) {
        return null;
      }

      result = queryExecutor.executeQuery(queryBundle.getQuery(),
                                          queryBundle.getMetaQuery(),
                                          queryBundle.getQueueContext());
    }
    while(result == null);

    return result;
  }

  /**
   * This method should be called from the {@link com.datatorrent.api.Operator#setup} method so that the
 QueryManagerSynchronous can correctly initialize its internal state.
   * @param context The operator context.
   */
  @Override
  public void setup(OperatorContext context)
  {
    queryQueueManager.setup(context);
  }

  /**
   * This method should be called from the {@link com.datatorrent.api.Operator#beginWindow} method so that the
 QueryManagerSynchronous can correctly initialize/reset its internal state at the beginning of each window.
   * @param windowId The windowId of the current window.
   */
  public void beginWindow(long windowId)
  {
    queryQueueManager.beginWindow(windowId);
  }

  /**
   * This method should be called from the {@link com.datatorrent.api.Operator#endWindow} method so that the
 QueryManagerSynchronous can correctly initialize/reset its internal state at the end of each window.
   */
  public void endWindow()
  {
    queryQueueManager.endWindow();
  }

  /**
   * This method should be called from the {@link com.datatorrent.api.Operator#teardown} method so that the
 QueryManagerSynchronous can correctly initialize its internal state.
   */
  @Override
  public void teardown()
  {
    queryQueueManager.teardown();
  }

  /**
   * Creates a new instance of a QueryManagerSynchronous.
   * @param <QUERY_TYPE> The type of the query.
   * @param <META_QUERY> The type of any meta data associated with the query.
   * @param <QUEUE_CONTEXT> The type of any additional meta data required to manage queueing the query.
   * @param <RESULT> The type of any query results.
   * @param queryExecutor The {@link QueryExecutor} the queryExecutor used to execute queries.
   * @return A new instance of QueryManagerSynchronous.
   */
  public static <QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT>
  QueryManagerSynchronous<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT>
  newInstance(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryExecutor)
  {
    return new QueryManagerSynchronous<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT>(queryExecutor);
  }

  /**
   * Creates a new instance of a QueryManagerSynchronous.
   * @param <QUERY_TYPE> The type of the query.
   * @param <META_QUERY> The type of any meta data associated with the query.
   * @param <QUEUE_CONTEXT> The type of any additional meta data required to manage queueing the query.
   * @param <RESULT> The type of any query results.
   * @param queryExecutor The {@link QueryExecutor} used to execute queries.
   * @param queryQueueManager The {@link QueueManager} used to queue queries.
   * @return A new instance of QueryManagerSynchronous.
   */
  public static <QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT>
  QueryManagerSynchronous<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT>
  newInstance(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryExecutor,
              QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager)
  {
    return new QueryManagerSynchronous<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT>(queryExecutor,
      queryQueueManager);
  }
}
