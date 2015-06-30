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

import org.apache.commons.lang3.mutable.MutableLong;

import com.datatorrent.lib.appdata.query.QueueList.QueueListNode;

/**
 * The WindowEndQueueManager keeps a countdown representing the number of application windows a query should stay alive for. If
 * a query's countdown reaches 0 it is removed from the queue.
 * @param <QUERY_TYPE> The type of queries to be queued.
 * @param <META_QUERY> The type of metadata to be associated with queued queries.
 */
public class WindowEndQueueManager<QUERY_TYPE, META_QUERY> extends AbstractWindowEndQueueManager<QUERY_TYPE, META_QUERY, MutableLong>
{
  public WindowEndQueueManager()
  {
  }

  @Override
  public boolean removeBundle(QueryBundle<QUERY_TYPE, META_QUERY, MutableLong> queryQueueable)
  {
    return queryQueueable.getQueueContext().longValue() <= 0L;
  }

  @Override
  public void endWindow()
  {
    for(QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, MutableLong>> tempNode = queryQueue.getHead();
        tempNode != null;
        tempNode = tempNode.getNext())
    {
      MutableLong qc = tempNode.getPayload().getQueueContext();
      qc.decrement();
    }
  }

  @Override
  public void addedNode(QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, MutableLong>> queryQueueable)
  {
    //Do nothing
  }

  @Override
  public void removedNode(QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, MutableLong>> queryQueueable)
  {
    //Do nothing
  }

  @Override
  public boolean addingFilter(QueryBundle<QUERY_TYPE, META_QUERY, MutableLong> queryBundle)
  {
    return true;
  }
}
