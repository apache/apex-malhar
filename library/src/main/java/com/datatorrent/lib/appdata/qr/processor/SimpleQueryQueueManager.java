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
package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Context.OperatorContext;

import java.util.LinkedList;

public class SimpleQueryQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>
                      implements QueryQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>
{
  private LinkedList<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> queue =
  new LinkedList<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>>();

  public SimpleQueryQueueManager()
  {
  }

  @Override
  public boolean enqueue(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT queueContext)
  {
    QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> qq =
    new QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>(query, metaQuery, queueContext);
    return queue.offer(qq);
  }

  @Override
  public QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> dequeue()
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
