/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class QueryProcessor<QUEUE_CONTEXT> implements Operator
{
  private QueryComputer queryComputer;
  private QueryQueueManager<QUEUE_CONTEXT> queryQueueManager;
  private QueryResultCacheManager queryResultCacheManager;

  public QueryProcessor(QueryComputer queryComputer)
  {
    setQueryComputer(queryComputer);
    queryQueueManager = new SimpleQueryQueueManager<QUEUE_CONTEXT>();
    queryResultCacheManager = new NOPQueryResultCacheManager();
  }

  public QueryProcessor(QueryComputer queryComputer,
                        QueryQueueManager queryQueueManager)
  {
    setQueryComputer(queryComputer);
    setQueryQueueManager(queryQueueManager);
    queryResultCacheManager = new NOPQueryResultCacheManager();
  }

  public QueryProcessor(QueryComputer queryComputer,
                        QueryResultCacheManager queryResultCacheManager)
  {
    setQueryComputer(queryComputer);
    setQueryResultCacheManager(queryResultCacheManager);
    queryQueueManager = new SimpleQueryQueueManager();
  }

  public QueryProcessor(QueryComputer queryComputer,
                        QueryQueueManager queryQueueManager,
                        QueryResultCacheManager queryResultCacheManager)
  {
    setQueryComputer(queryComputer);
    setQueryQueueManager(queryQueueManager);
    setQueryResultCacheManager(queryResultCacheManager);
  }

  private void setQueryComputer(QueryComputer queryComputer)
  {
    Preconditions.checkNotNull(queryComputer);
    this.queryComputer = queryComputer;
  }

  private void setQueryQueueManager(QueryQueueManager queryQueueManager)
  {
    Preconditions.checkNotNull(queryQueueManager);
    this.queryQueueManager = queryQueueManager;
  }

  private void setQueryResultCacheManager(QueryResultCacheManager queryResultCacheManager)
  {
    Preconditions.checkNotNull(queryResultCacheManager);
    this.queryResultCacheManager = queryResultCacheManager;
  }

  public boolean enqueue(Query query, QUEUE_CONTEXT queueContext)
  {
    return queryQueueManager.enqueue(query, queueContext);
  }

  public Result process()
  {
    Query query = queryQueueManager.dequeue();
    Result result = queryResultCacheManager.getResult(query);

    if(result != null) {
      return result;
    }

    return queryComputer.processQuery(query);
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
