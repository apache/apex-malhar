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

import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.commons.lang3.mutable.MutableLong;

import com.datatorrent.lib.appdata.query.QueueList.QueueListNode;
import com.datatorrent.lib.appdata.schemas.Query;

/**
 * This {@link QueueManager} behaves like {@link WindowEndQueueManager} except that if another query is added to the queue with
 * the same query id as an existing query in the queue, the existing query is replaced with the new query.
 * @param <QUERY> The type of queries kept in the queue.
 * @param <META_QUERY> The type of query meta data kept in the queue.
 */
public class AppDataWindowEndQueueManager<QUERY extends Query, META_QUERY> extends WindowEndQueueManager<QUERY, META_QUERY>
{
  /**
   * A map from query IDs to queue nodes.
   */
  private final Map<String, QueueListNode<QueryBundle<QUERY, META_QUERY, MutableLong>>> queryIDToNode = Maps.newHashMap();

  public AppDataWindowEndQueueManager()
  {
  }

  @Override
  public boolean enqueue(QUERY query, META_QUERY metaQuery, MutableLong context)
  {
    if(context != null) {
      query.setCountdown(context.getValue());
    }

    if(query.isOneTime()) {
      return super.enqueue(query, metaQuery, new MutableLong(1L));
    }
    else {
      return super.enqueue(query, metaQuery, new MutableLong(query.getCountdown()));
    }
  }

  @Override
  public void addedNode(QueueListNode<QueryBundle<QUERY, META_QUERY, MutableLong>> queryQueueable)
  {
    queryIDToNode.put(queryQueueable.getPayload().getQuery().getId(), queryQueueable);
  }

  @Override
  public void removedNode(QueueListNode<QueryBundle<QUERY, META_QUERY, MutableLong>> queryQueueable)
  {
    queryIDToNode.remove(queryQueueable.getPayload().getQuery().getId());
  }

  @Override
  public boolean addingFilter(QueryBundle<QUERY, META_QUERY, MutableLong> queryBundle)
  {
    QueueListNode<QueryBundle<QUERY, META_QUERY, MutableLong>> queryNode =
    queryIDToNode.get(queryBundle.getQuery().getId());

    if(queryNode == null) {
      return true;
    }

    queryNode.setPayload(queryBundle);

    return false;
  }
}
