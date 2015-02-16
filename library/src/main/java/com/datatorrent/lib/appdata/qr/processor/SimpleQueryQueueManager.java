/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.qr.Query;
import java.util.LinkedList;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SimpleQueryQueueManager<QUERY_TYPE extends Query, META_QUERY, QUEUE_CONTEXT>
                      implements QueryQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>
{
  private LinkedList<QueryQueueable<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> queue =
  new LinkedList<QueryQueueable<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>>();

  public SimpleQueryQueueManager()
  {
  }

  @Override
  public boolean enqueue(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT queueContext)
  {
    QueryQueueable<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> qq =
    new QueryQueueable<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>(query, metaQuery, queueContext);
    return queue.offer(qq);
  }

  @Override
  public QueryBundle<QUERY_TYPE, META_QUERY> dequeue()
  {
    return queue.poll();
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void teardown()
  {
  }
}
