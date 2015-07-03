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

import com.google.common.base.Preconditions;

/**
 * This is a doubly linked list to be used for queueing queries.
 * @param <T> The type of the data used for queueing
 */
public class QueueList<T>
{
  /**
   * The number of nodes in the {@link QueueList}.
   */
  private int size = 0;

  /**
   * The head of the queue.
   */
  private QueueListNode<T> head;
  /**
   * The tail of the queue.
   */
  private QueueListNode<T> tail;

  /**
   * A lock which is used to make queueing and dequeueing from this {@link QueueList} thread safe.
   */
  private final Object lock = new Object();

  /**
   * Create a queue list.
   */
  public QueueList()
  {
    //Do nothing
  }

  /**
   * Enqueues the given node to the list.
   * @param node The node to enqueue in the list.
   */
  public void enqueue(QueueListNode<T> node)
  {
    synchronized(lock) {
      Preconditions.checkNotNull(node);
      size++;

      if(head == null) {
        head = node;
        tail = node;
        node.setNext(null);
        node.setPrev(null);
        return;
      }

      tail.setNext(node);
      node.setPrev(tail);
      node.setNext(null);
      tail = node;
    }
  }

  /**
   * Returns the head of the QueueList.
   * @return The head node of the queue list.
   */
  public QueueListNode<T> getHead()
  {
    synchronized(lock) {
      return head;
    }
  }

  /**
   * Removes the given node from the QueueList.
   * @param node The node to remove from this QueueList.
   */
  public void removeNode(QueueListNode<T> node)
  {
    synchronized(lock) {
      size--;

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

  /**
   * Returns the size of the {@link QueueList}.
   * @return The size of the {@link QueueList}.
   */
  public int getSize()
  {
    return size;
  }

  /**
   * This class represents a node in the QueueList, and holds a data payload provided by the user of type T.
   * @param <T> The type of the data payload of the node.
   */
  public static class QueueListNode<T>
  {
    private QueueListNode<T> prev;
    private QueueListNode<T> next;
    private T payload;

    /**
     * Creates a new queue list node with node data payload.
     */
    public QueueListNode()
    {
    }

    /**
     * Creates a new queue list node with the given data payload.
     * @param payload The data payload for this node.
     */
    public QueueListNode(T payload)
    {
      this.payload = payload;
    }

    /**
     * Gets the previous node that this node points to.
     * @return The previous node that this node points to.
     */
    public QueueListNode<T> getPrev()
    {
      return prev;
    }

    /**
     * Sets the previous node that this node points to.
     * @param prev The previous node that this node will point to.
     */
    public void setPrev(QueueListNode<T> prev)
    {
      this.prev = prev;
    }

    /**
     * Gets the next element this node points to.
     * @return The next element this node points to.
     */
    public QueueListNode<T> getNext()
    {
      return next;
    }

    /**
     * Sets the next element this node points to.
     * @param next The next element this node points to.
     */
    public void setNext(QueueListNode<T> next)
    {
      this.next = next;
    }

    /**
     * Gets the data payload for the node.
     * @return The data payload for the node.
     */
    public T getPayload()
    {
      return payload;
    }

    /**
     * Sets the data payload for the node.
     * @param payload The data payload for the node.
     */
    public void setPayload(T payload)
    {
      this.payload = payload;
    }

    @Override
    public String toString()
    {
      return "QueueListNode{" + "payload=" + payload + '}';
    }
  }
}

