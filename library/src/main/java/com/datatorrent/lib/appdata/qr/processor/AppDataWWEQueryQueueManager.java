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

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.processor.QueueList.QueueListNode;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Map;

public class AppDataWWEQueryQueueManager<QUERY extends Query, META_QUERY> extends WWEQueryQueueManager<QUERY, META_QUERY>
{
  private Map<String, QueueListNode<QueryBundle<QUERY, META_QUERY, MutableLong>>> queryIDToNode = Maps.newHashMap();

  public AppDataWWEQueryQueueManager()
  {
  }

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

    /*if(queryNode.getPayload().getQuery().queueEquals(queryBundle.getQuery())) {
      //Old query equals new query we want to to keep existing asynchronous queries active.
      //TODO need to generate
      queryNode.getPayload().getQueueContext().setValue(queryBundle.getQueueContext().getValue());
    }
    else {*/
      //Otherwise replace existing query.

      queryNode.setPayload(queryBundle);
    //}

    return false;
  }
}
