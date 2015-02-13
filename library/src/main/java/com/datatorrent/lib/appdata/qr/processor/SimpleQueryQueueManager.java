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
public class SimpleQueryQueueManager<QUEUE_CONTEXT> implements QueryQueueManager<QUEUE_CONTEXT>
{
  private LinkedList<Query> queue = new LinkedList<Query>();

  public SimpleQueryQueueManager()
  {
  }

  @Override
  public boolean enqueue(Query query, QUEUE_CONTEXT queueContext)
  {
    return queue.offer(query);
  }

  @Override
  public Query dequeue()
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
