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
import com.datatorrent.lib.appdata.qr.processor.QueueList.QueueListNode;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractWEQueryQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> implements QueryQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractWEQueryQueueManager.class);

  protected QueueList<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> queryQueue =
  new QueueList<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>>();
  private QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> currentNode;
  private boolean readCurrent = false;

  private final Object lock = new Object();

  public AbstractWEQueryQueueManager()
  {
  }

  @Override
  public boolean enqueue(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT context)
  {
    Preconditions.checkNotNull(query);

    synchronized(lock) {
      return enqueueHelper(query, metaQuery, context);
    }
  }

  private boolean enqueueHelper(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT context)
  {
    QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueable =
    new QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>(query, metaQuery, context);

    QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> node = new QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>>(queryQueueable);

    if(addingFilter(queryQueueable)) {
      queryQueue.enqueue(node);
      addedNode(node);
    }

    return true;
  }

  @Override
  public QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> dequeue()
  {
    synchronized(lock) {
      return dequeueHelper();
    }
  }

  private QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> dequeueHelper()
  {
    QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> qq = null;

    if(currentNode == null) {
      currentNode = queryQueue.getHead();
      readCurrent = false;

      if(currentNode == null) {
        return null;
      }
    }
    else {
      if(readCurrent) {
        QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> tempNode = currentNode.getNext();

        if(tempNode != null) {
          currentNode = tempNode;
          readCurrent = false;
        }
        else {
          return null;
        }
      }
    }

    while(true)
    {
      QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueable = currentNode.getPayload();
      QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> nextNode = currentNode.getNext();

      if(removeBundle(queryQueueable)) {
        queryQueue.removeNode(currentNode);
        removedNode(currentNode);
      }
      else {
        qq = currentNode.getPayload();
      }

      if(nextNode == null) {
        readCurrent = true;
        break;
      }

      currentNode = nextNode;

      if(qq != null) {
        break;
      }
    }

    return qq;
  }

  public abstract boolean addingFilter(QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryBundle);
  public abstract void addedNode(QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> queryQueueable);
  public abstract void removedNode(QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> queryQueueable);
  public abstract boolean removeBundle(QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueable);

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentNode = queryQueue.getHead();
    readCurrent = false;
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
