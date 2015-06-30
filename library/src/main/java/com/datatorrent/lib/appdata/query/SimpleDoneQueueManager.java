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

import java.util.concurrent.Semaphore;

import org.apache.commons.lang3.mutable.MutableBoolean;

import com.datatorrent.lib.appdata.query.QueueList.QueueListNode;

/**
 * This is simple queue whose queue context is a {@link MutableBoolean} which determines whether or not to keep the query in the queue. If the
 * queue context is true, then the query is remove, if the queue context is false, then the query stays in the queue.
 * @param <QUERY_TYPE> The type of the queries that are enqueued in the queue.
 * @param <META_QUERY> The type of any meta data associated with the queries.
 */
public class SimpleDoneQueueManager<QUERY_TYPE, META_QUERY> extends
AbstractWindowEndQueueManager<QUERY_TYPE, META_QUERY, MutableBoolean>
{
  private QueueList<QueryBundle<QUERY_TYPE, META_QUERY, MutableBoolean>> queryQueue;
  private Semaphore semaphore = new Semaphore(1);

  public SimpleDoneQueueManager()
  {
  }

  @Override
  public boolean removeBundle(QueryBundle<QUERY_TYPE, META_QUERY, MutableBoolean> queryQueueable)
  {
    return queryQueueable.getQueueContext().booleanValue();
  }

  @Override
  public void addedNode(QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, MutableBoolean>> queryQueueable)
  {
    semaphore.release();
  }

  @Override
  public void removedNode(QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, MutableBoolean>> queryQueueable)
  {
    //Do nothing
  }

  @Override
  public boolean addingFilter(QueryBundle<QUERY_TYPE, META_QUERY, MutableBoolean> queryBundle)
  {
    return true;
  }
}
