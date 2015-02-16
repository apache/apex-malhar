/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.qr.Query;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class WEQueryQueueManager<QUERY_TYPE extends Query, META_QUERY>
implements QueryQueueManager<QUERY_TYPE, META_QUERY, Long>
{
  private static final Logger logger = LoggerFactory.getLogger(WEQueryQueueManager.class);

  private long windowCounter = 0L;
  private WELinkedList<QueryQueueable<QUERY_TYPE, META_QUERY, WEQueueContext>> queryQueue =
  new WELinkedList<QueryQueueable<QUERY_TYPE, META_QUERY, WEQueueContext>>();
  private WELinkedListNode<QueryQueueable<QUERY_TYPE, META_QUERY, WEQueueContext>> currentNode;
  private boolean readCurrent = false;

  private final Object lock = new Object();

  @Override
  public boolean enqueue(QUERY_TYPE query, META_QUERY metaQuery, Long windowExpireCount)
  {
    Preconditions.checkNotNull(query);
    Preconditions.checkNotNull(windowExpireCount);

    synchronized(lock) {
      return enqueueHelper(query, metaQuery, windowExpireCount);
    }
  }

  private boolean enqueueHelper(QUERY_TYPE query, META_QUERY metaQuery, Long windowExpireCount)
  {
    WEQueueContext context = new WEQueueContext(windowExpireCount, windowCounter);
    QueryQueueable<QUERY_TYPE, META_QUERY, WEQueueContext> queryQueueable =
    new QueryQueueable<QUERY_TYPE, META_QUERY, WEQueueContext>(query, metaQuery, context);

    queryQueue.enqueue(new WELinkedListNode<QueryQueueable<QUERY_TYPE, META_QUERY, WEQueueContext>>(queryQueueable));

    return true;
  }

  @Override
  public QueryBundle<QUERY_TYPE, META_QUERY> dequeue()
  {
    synchronized(lock) {
      return dequeueHelper();
    }
  }

  private QueryBundle<QUERY_TYPE, META_QUERY> dequeueHelper()
  {
    QueryQueueable<QUERY_TYPE, META_QUERY, WEQueueContext> qq = null;

    if(currentNode == null) {
      currentNode = queryQueue.getHead();
      readCurrent = false;

      if(currentNode == null) {
        return null;
      }
    }
    else {
      if(readCurrent) {
        WELinkedListNode<QueryQueueable<QUERY_TYPE, META_QUERY, WEQueueContext>> tempNode = currentNode.getNext();

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
      QueryQueueable<QUERY_TYPE, META_QUERY, WEQueueContext> queryQueueable = currentNode.getPayload();
      long numberOfWindowsAvailableFor = queryQueueable.getQueueContext().getNumberOfWindowsAvailableFor();
      long enqueueWindowCounter = queryQueueable.getQueueContext().getEnqueueWindowCounter();

      long diff = windowCounter - enqueueWindowCounter;

      WELinkedListNode<QueryQueueable<QUERY_TYPE, META_QUERY, WEQueueContext>> nextNode = currentNode.getNext();

      if(diff >= numberOfWindowsAvailableFor) {
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

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    windowCounter++;
    currentNode = queryQueue.getHead();
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void teardown()
  {
  }

  private static class WEQueueContext
  {
    private long numberOfWindowsAvailableFor;
    private long enqueueWindowCounter;

    public WEQueueContext(long numberOfWindowsAvailableFor,
                          long enqueueWindowCount)
    {
      Preconditions.checkArgument(numberOfWindowsAvailableFor > 0, "The windowCounter must be positive.");
      this.numberOfWindowsAvailableFor = numberOfWindowsAvailableFor;
      this.enqueueWindowCounter = enqueueWindowCount;
    }

    /**
     * @return the currentWindowCounter
     */
    public long getEnqueueWindowCounter()
    {
      return enqueueWindowCounter;
    }

    /**
     * @param enqueueWindowCounter the currentWindowCounter to set
     */
    public void setEnqueueWindowCounter(long enqueueWindowCounter)
    {
      this.enqueueWindowCounter = enqueueWindowCounter;
    }

    /**
     * @return the numberOfWindowsAvailableFor
     */
    public long getNumberOfWindowsAvailableFor()
    {
      return numberOfWindowsAvailableFor;
    }

    /**
     * @param numberOfWindowsAvailableFor the numberOfWindowsAvailableFor to set
     */
    public void setNumberOfWindowsAvailableFor(long numberOfWindowsAvailableFor)
    {
      this.numberOfWindowsAvailableFor = numberOfWindowsAvailableFor;
    }
  }

  private static class WELinkedList<T>
  {
    private WELinkedListNode<T> head;
    private WELinkedListNode<T> tail;

    public WELinkedList()
    {
    }

    public void enqueue(WELinkedListNode<T> node)
    {
      Preconditions.checkNotNull(node);

      if(head == null) {
        head = node;
        tail = node;
        node.setNext(null);
        node.setPrev(null);
        return;
      }

      //Handle the case when adding to the end of list and
      //removing a node in parallel
      tail.setNext(node);
      node.setPrev(tail);
      node.setNext(null);
      tail = node;
    }

    public WELinkedListNode<T> getHead()
    {
      return head;
    }

    public void removeNode(WELinkedListNode<T> node)
    {
      //Handle the case when adding to the end of list and
      //removing a node in parallel
      if(head == node) {
        if(tail == node) {
          head = null;
          tail = null;
        }
        else {
          head = node.getNext();
          head.setPrev(null);
        }
      }
      else {
        if(tail == node) {
          tail = node.getPrev();
          tail.setNext(null);
        }
        else {
          node.getPrev().setNext(node.getNext());
          node.getNext().setPrev(node.getPrev());
        }
      }
    }
  }

  private static class WELinkedListNode<T>
  {
    private WELinkedListNode<T> prev;
    private WELinkedListNode<T> next;
    private T payload;

    public WELinkedListNode()
    {
    }

    public WELinkedListNode(T payload)
    {
      this.payload = payload;
    }

    public WELinkedListNode<T> getPrev()
    {
      return prev;
    }

    public void setPrev(WELinkedListNode<T> prev)
    {
      this.prev = prev;
    }

    /**
     * @return the next
     */
    public WELinkedListNode<T> getNext()
    {
      return next;
    }

    /**
     * @param next the next to set
     */
    public void setNext(WELinkedListNode<T> next)
    {
      this.next = next;
    }

    /**
     * @return the payload
     */
    public T getPayload()
    {
      return payload;
    }

    /**
     * @param payload the payload to set
     */
    public void setPayload(T payload)
    {
      this.payload = payload;
    }

    @Override
    public String toString()
    {
      return "WELinkedListNode{" + "payload=" + payload + '}';
    }
  }
}
