/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class QueryProcessor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

  private QueryComputer<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer;
  private QueryQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager;
  private QueryResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> queryResultCacheManager;

  public QueryProcessor(QueryComputer<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer)
  {
    setQueryComputer(queryComputer);
    queryQueueManager = new SimpleQueryQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>();
    queryResultCacheManager = new NOPQueryResultCacheManager<QUERY_TYPE, META_QUERY, RESULT>();
  }

  public QueryProcessor(QueryComputer<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer,
                        QueryQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager)
  {
    setQueryComputer(queryComputer);
    setQueryQueueManager(queryQueueManager);
    queryResultCacheManager = new NOPQueryResultCacheManager<QUERY_TYPE, META_QUERY, RESULT>();
  }

  public QueryProcessor(QueryComputer<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer,
                        QueryResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> queryResultCacheManager)
  {
    setQueryComputer(queryComputer);
    setQueryResultCacheManager(queryResultCacheManager);
    queryQueueManager = new SimpleQueryQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>();
  }

  public QueryProcessor(QueryComputer<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer,
                        QueryQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager,
                        QueryResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> queryResultCacheManager)
  {
    setQueryComputer(queryComputer);
    setQueryQueueManager(queryQueueManager);
    setQueryResultCacheManager(queryResultCacheManager);
  }

  private void setQueryComputer(QueryComputer<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT> queryComputer)
  {
    Preconditions.checkNotNull(queryComputer);
    this.queryComputer = queryComputer;
  }

  private void setQueryQueueManager(QueryQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueManager)
  {
    Preconditions.checkNotNull(queryQueueManager);
    this.queryQueueManager = queryQueueManager;
  }

  private void setQueryResultCacheManager(QueryResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> queryResultCacheManager)
  {
    Preconditions.checkNotNull(queryResultCacheManager);
    this.queryResultCacheManager = queryResultCacheManager;
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

    return queryComputer.processQuery(queryBundle.getQuery(),
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
}
