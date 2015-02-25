/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class QueueList<T>
{
  private QueueListNode<T> head;
  private QueueListNode<T> tail;

  public QueueList()
  {
  }

  public void enqueue(QueueListNode<T> node)
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

  public QueueListNode<T> getHead()
  {
    return head;
  }

  public void removeNode(QueueListNode<T> node)
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

  public static class QueueListNode<T>
  {
    private QueueListNode<T> prev;
    private QueueListNode<T> next;
    private T payload;

    public QueueListNode()
    {
    }

    public QueueListNode(T payload)
    {
      this.payload = payload;
    }

    public QueueListNode<T> getPrev()
    {
      return prev;
    }

    public void setPrev(QueueListNode<T> prev)
    {
      this.prev = prev;
    }

    /**
     * @return the next
     */
    public QueueListNode<T> getNext()
    {
      return next;
    }

    /**
     * @param next the next to set
     */
    public void setNext(QueueListNode<T> next)
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

