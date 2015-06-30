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
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.QueueUtils.ConditionBarrier;
import com.datatorrent.lib.appdata.query.QueueList.QueueListNode;

import com.datatorrent.api.Context.OperatorContext;

/**
 * This is an abstract implementation of a QueueManager which works in the following way.
 * <br/>
 * <br/>
 * <ul>
 *  <li>All the queries are kept in a linked list.</li>
 *  <li>A pointer points to the next query to pop from the queue.</li>
 *  <li>If all the queries are popped within an application window, then no more queries can be popped from the queue</li>
 *  <li>When {@link #endWindow} is called</li>
 * </ul>
 * @param <QUERY_TYPE> The type of the query.
 * @param <META_QUERY> The type of any metadata associated with the query.
 * @param <QUEUE_CONTEXT> The type of the context used to manage the queueing of the query.
 */
public abstract class AbstractWindowEndQueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> implements QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>
{
  /**
   * The {@link QueueList} which is backing this {@link QueueManager}.
   */
  protected QueueList<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> queryQueue =
  new QueueList<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>>();
  /**
   * A pointer to the current node in the {@link QueueList}.
   */
  private QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> currentNode;
  private boolean readCurrent = false;

  private final Semaphore semaphore = new Semaphore(0);
  private final ConditionBarrier conditionBarrier = new ConditionBarrier();
  private final AtomicInteger numLeft = new AtomicInteger();

  /**
   * Creates a new QueueManager.
   */
  public AbstractWindowEndQueueManager()
  {
    //Do nothing
  }

  @Override
  public boolean enqueue(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT context)
  {
    Preconditions.checkNotNull(query);

    conditionBarrier.gate();

    return enqueueHelper(query, metaQuery, context);
  }

  /**
   * This is a helper method which enqueues the query.
   * @param query The query to enqueue.
   * @param metaQuery Any meta data associated with the query.
   * @param context The context used to manage queuing the query.
   * @return True if the query was successfully enqueued. False otherwise.
   */
  private boolean enqueueHelper(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT context)
  {
    QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueable =
    new QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>(query, metaQuery, context);

    QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> node = new QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>>(queryQueueable);

    if(addingFilter(queryQueueable)) {
      queryQueue.enqueue(node);
      numLeft.getAndIncrement();
      semaphore.release();

      addedNode(node);
    }

    return true;
  }

  @Override
  public QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> dequeue()
  {
    return dequeueHelper(false);
  }

  @Override
  public QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> dequeueBlock()
  {
    return dequeueHelper(true);
  }

  /**
   * This is a helper method for dequeueing queries.
   * @return {@link QueryBundle} containing the dequeued query and its associated metadata.
   */
  private QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> dequeueHelper(boolean block)
  {
    QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> qq = null;

    boolean first = true;

    if(block) {
      acquire();
    }

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
      if(block && !first) {
        acquire();

        //TODO dedup this code
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
      }

      synchronized(numLeft) {
        numLeft.getAndDecrement();
        QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueable = currentNode.getPayload();

        first = false;

        QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> nextNode = currentNode.getNext();

        if(removeBundle(queryQueueable)) {
          queryQueue.removeNode(currentNode);
          removedNode(currentNode);

          if(block) {
            if(nextNode == null) {
              readCurrent = true;
            }
            else {
              currentNode = nextNode;
              readCurrent = false;
            }
          }
          else {
            if(nextNode == null) {
              readCurrent = true;
              break;
            }
            else {
              currentNode = nextNode;
            }
          }
        }
        else {
          qq = currentNode.getPayload();

          if(nextNode == null) {
            readCurrent = true;
          }
          else {
            currentNode = nextNode;
            readCurrent = false;
          }

          break;
        }
      }
    }

    //Handle the case where non blocking dequeue is happening, semaphore needs to be synched up.
    if(semaphore.availablePermits() > numLeft.get()) {
      try {
        semaphore.acquire(semaphore.availablePermits() - numLeft.get());
      }
      catch(InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }

    return qq;
  }

  private void acquire()
  {
    try {
      semaphore.acquire();
    }
    catch(InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * This filter is used to determine whether or not the given {@link QueryBundle} should be added to the queue or not.
   * @param queryBundle The {@link QueryBundle} to determine whether or not to add to the queue.
   * @return True if the given {@link QueryBundle} should be added to the queue. False otherwise.
   */
  public abstract boolean addingFilter(QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryBundle);

  /**
   * This callback is called when a new query is enqueued.
   * @param queryQueueable The node that was added to the {@link QueueList} of the queue.
   */
  public abstract void addedNode(QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> queryQueueable);

  /**
   * This callback is called with the given node is removed from the queue.
   * @param queryQueueable The node that was removed from the {@link QueueList}.
   */
  public abstract void removedNode(QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>> queryQueueable);

  /**
   * This is called to determine if the given query should be removed from the queue.
   * @param queryQueueable The {@link QueryBundle} whose removal from the queue needs to be determined.
   * @return True if the given query should be remove. False otherwise.
   */
  public abstract boolean removeBundle(QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryQueueable);

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    synchronized(numLeft) {
      currentNode = queryQueue.getHead();
      readCurrent = false;
      numLeft.set(queryQueue.getSize());
      semaphore.drainPermits();
      semaphore.release(queryQueue.getSize());
    }
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
    return numLeft.get();
  }

  @VisibleForTesting
  int getNumPermits()
  {
    return semaphore.availablePermits();
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

  private static final Logger LOG = LoggerFactory.getLogger(AbstractWindowEndQueueManager.class);
}
