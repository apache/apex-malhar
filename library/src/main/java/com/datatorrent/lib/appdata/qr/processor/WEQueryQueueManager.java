/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.qr.Query;
import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class WEQueryQueueManager implements QueryQueueManager<Long>
{
  private long windowCounter = 0L;
  private WELinkedList<QueryQueueable<WEQueueContext>> queryQueue = new WELinkedList<QueryQueueable<WEQueueContext>>();
  private WELinkedListNode<QueryQueueable<WEQueueContext>> currentNode;

  @Override
  public boolean enqueue(Query query, Long windowExpireCount)
  {
    WEQueueContext context = new WEQueueContext(windowExpireCount, windowCounter);
    QueryQueueable<WEQueueContext> queryQueueable = new QueryQueueable<WEQueueContext>(query, context);

    queryQueue.enqueue(new WELinkedListNode<QueryQueueable<WEQueueContext>>(queryQueueable));
    return true;
  }

  @Override
  public Query dequeue()
  {
    Query query = null;

    while(currentNode != null &&
          query == null)
    {
      QueryQueueable<WEQueueContext> queryQueueable = currentNode.getPayload();
      long numberOfWindowsAvailableFor = queryQueueable.getQueueContext().getNumberOfWindowsAvailableFor();
      long enqueueWindowCounter = queryQueueable.getQueueContext().getEnqueueWindowCounter();

      long diff = windowCounter - enqueueWindowCounter;

      if(diff >= numberOfWindowsAvailableFor) {
        WELinkedListNode<QueryQueueable<WEQueueContext>> nextNode = currentNode.getNext();
        queryQueue.removeNode(currentNode);
        currentNode = nextNode;
      }
      else {
        query = currentNode.getPayload().getQuery();
        currentNode = currentNode.getNext();
      }
    }

    return query;
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
      synchronized(tail) {
        tail.setNext(node);
        node.setPrev(tail);
        node.setNext(null);
        tail = node;
      }
    }

    public WELinkedListNode<T> getHead()
    {
      return head;
    }

    public void removeNode(WELinkedListNode<T> node)
    {
      //Handle the case when adding to the end of list and
      //removing a node in parallel
      synchronized(node) {
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
  }
}
