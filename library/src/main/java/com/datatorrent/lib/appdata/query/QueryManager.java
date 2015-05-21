/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;

/**
 * Use {@link #newInstance} to create an instance of query processor. It reduces the boilerplate code with respect to generics.
 *
 * @param <QUERY_TYPE>
 * @param <META_QUERY>
 * @param <QUEUE_CONTEXT>
 * @param <COMPUTE_CONTEXT>
 * @param <RESULT>
 */
public class QueryManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(QueryManager.class);

  private QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer;
  private QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager;
  private ResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> queryResultCacheManager;

  private QueryManager(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer)
  {
    setQueryComputer(queryComputer);
    queryQueueManager = new SimpleQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>();
    queryResultCacheManager = new NOPQueryResultCacheManager<QUERY_TYPE, META_QUERY, RESULT>();
  }

  private QueryManager(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer,
                        QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager)
  {
    setQueryComputer(queryComputer);
    setQueryQueueManager(queryQueueManager);
    queryResultCacheManager = new NOPQueryResultCacheManager<QUERY_TYPE, META_QUERY, RESULT>();
  }

  private QueryManager(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer,
                        ResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> queryResultCacheManager)
  {
    setQueryComputer(queryComputer);
    setQueryResultCacheManager(queryResultCacheManager);
    queryQueueManager = new SimpleQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>();
  }

  private QueryManager(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer,
                        QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager,
                        ResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> queryResultCacheManager)
  {
    setQueryComputer(queryComputer);
    setQueryQueueManager(queryQueueManager);
    setQueryResultCacheManager(queryResultCacheManager);
  }

  private void setQueryComputer(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer)
  {
    this.queryComputer = Preconditions.checkNotNull(queryComputer);
  }

  private void setQueryQueueManager(QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager)
  {
    this.queryQueueManager = Preconditions.checkNotNull(queryQueueManager);
  }

  private void setQueryResultCacheManager(ResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> queryResultCacheManager)
  {
    this.queryResultCacheManager = Preconditions.checkNotNull(queryResultCacheManager);
  }

  public boolean enqueue(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT queueContext)
  {
    return queryQueueManager.enqueue(query, metaQuery, queueContext);
  }

  public RESULT process(COMPUTE_CONTEXT context)
  {
    QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryBundle = queryQueueManager.dequeue();

    if(queryBundle == null) {
      queryComputer.queueDepleted(context);
      return null;
    }

    RESULT result = queryResultCacheManager.getResult(queryBundle.getQuery(),
                                                      queryBundle.getMetaQuery());

    if(result != null) {
      return result;
    }

    return queryComputer.executeQuery(queryBundle.getQuery(),
                                      queryBundle.getMetaQuery(),
                                      queryBundle.getQueueContext(),
                                      context);
  }

  @Override
  public void setup(OperatorContext context)
  {
    queryQueueManager.setup(context);
    queryResultCacheManager.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    queryQueueManager.beginWindow(windowId);
    queryResultCacheManager.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    queryQueueManager.endWindow();
    queryResultCacheManager.endWindow();
  }

  @Override
  public void teardown()
  {
    queryQueueManager.teardown();
    queryResultCacheManager.teardown();
  }

  /**
   * Creates a new instance of query processor using query computer.
   */
  public static <QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>
  QueryManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>

  newInstance(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer)
  {
    return new QueryManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>(queryComputer);
  }

  /**
   * Creates a new instance of query processor using query computer and queue manager.
   */
  public static <QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>
  QueryManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>

  newInstance(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer,
              QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager)
  {
    return new QueryManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>(queryComputer,
      queryQueueManager);
  }

  /**
   * Creates a new instance of query processor using query computer and result cache manager.
   */
  public static <QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>
  QueryManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>

  newInstance(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer,
              ResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> queryResultCacheManager)
  {
    return new QueryManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>(queryComputer,
      queryResultCacheManager);
  }

  /**
   * Creates a new instance of query processor using query computer, queue manager & result cache manager.
   */
  public static <QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>
  QueryManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>

  newInstance(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer,
              QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager,
              ResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> queryResultCacheManager)
  {
    return new QueryManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>(queryComputer,
      queryQueueManager, queryResultCacheManager);
  }
}
