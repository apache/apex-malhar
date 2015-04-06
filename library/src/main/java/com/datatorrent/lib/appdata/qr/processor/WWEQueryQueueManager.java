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

import com.datatorrent.lib.appdata.qr.processor.QueueList.QueueListNode;
import org.apache.commons.lang3.mutable.MutableLong;

public class WWEQueryQueueManager<QUERY_TYPE, META_QUERY> extends AbstractWEQueryQueueManager<QUERY_TYPE, META_QUERY, MutableLong>
{
  public WWEQueryQueueManager()
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
