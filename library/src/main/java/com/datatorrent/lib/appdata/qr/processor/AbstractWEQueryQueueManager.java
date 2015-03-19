/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.qr.processor.QueueList.QueueListNode;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
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

    queryQueue.enqueue(new QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>>(queryQueueable));

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
    logger.debug("");

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
