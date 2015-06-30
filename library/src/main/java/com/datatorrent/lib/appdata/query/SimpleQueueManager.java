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

import java.util.LinkedList;
import java.util.concurrent.Semaphore;

import com.datatorrent.lib.appdata.QueueUtils.ConditionBarrier;

import com.datatorrent.api.Context.OperatorContext;

/**
 * This {@link QueueManager} functions like a standard {@link QueueManager}. Queries can be enqueued and when they are dequeued they are
 * completely removed from the queue.
 * @param <QUERY_TYPE> The type of the query to be enqueued in the queue.
 * @param <META_QUERY> The type of the meta data to be enqueued with the query.
 * @param <QUEUE_CONTEXT> The type of the queue context data.
 */
public class SimpleQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>
                      implements QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>
{
  private LinkedList<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> queue =
  new LinkedList<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>>();

  private final Semaphore semaphore = new Semaphore(0);
  private final ConditionBarrier conditionBarrier = new ConditionBarrier();

  public SimpleQueueManager()
  {
  }

  @Override
  public boolean enqueue(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT queueContext)
  {
    conditionBarrier.gate();

    QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> qq =
    new QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>(query, metaQuery, queueContext);

    if(queue.offer(qq)) {
      semaphore.release();
      return true;
    }

    return false;
  }

  @Override
  public QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> dequeue()
  {
    return queue.poll();
  }

  @Override
  public QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> dequeueBlock()
  {
    try {
      semaphore.acquire();
    }
    catch(InterruptedException ex) {
      throw new RuntimeException(ex);
    }

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

  @Override
  public int getNumLeft()
  {
    return queue.size();
  }

  @Override
  public void haltEnqueue()
  {
    conditionBarrier.lock();
  }

  @Override
  public void resumeEnqueue()
  {
    conditionBarrier.unlock();
  }
}
