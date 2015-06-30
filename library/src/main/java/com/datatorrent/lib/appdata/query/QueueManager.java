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

import com.datatorrent.api.Component;
import com.datatorrent.api.Context.OperatorContext;

/**
 * This is an interface for a manager which manages the queueing of AppData queries.
 * @param <QUERY_TYPE> The type of the queries being queued.
 * @param <META_QUERY> The type of any meta data to be queued with the query.
 * @param <QUEUE_CONTEXT> The type of any additional contextual information that could impact the way in
 * which a query is queued that is known when the query is queued. This queue context information could
 * be updated by the queue manager throughout the lifetime of the query to control things like how long
 * the query has been queued for.
 */
public interface QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> extends Component<OperatorContext>
{
  /**
   * This method enqueues an AppData query.
   * @param query The query to queue.
   * @param metaQuery Any additional metadata required by the query.
   * @param queueContext Any additional contextual information that will impact the way in which the query
   * is queued and is known when the query is queued.
   * @return True if the query was successfully queued. False otherwise.
   */
  public boolean enqueue(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT queueContext);
  /**
   * <p>
   * This method dequeues a query, and returns a {@link QueryBundle} which includes the query,
   * any additional query meta data, and the queue context for the query.
   * </p>
   * <p>
   * <b>Note:</b> Calls to {@link #dequeue} can be mixed with calls to {@link #dequeueBlock}.
   * </p>
   * @return The query bundle for a query.
   */
  public QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> dequeue();
  /**
   * This should be called in beginWindow of an operator so that the {@link QueueManager} can correctly update
   * its internal state for managing queries.
   * @param windowId The windowId of the current window.
   */
  public void beginWindow(long windowId);
  /**
   * This should be called in endWindow of an operator so that the {@link QueueManager} can correctly update its
   * internal state for managing queries.
   */
  public void endWindow();

  /**
   * <p>
   * Returns the next {@link QueryBundle} in the queue. If there is no new {@link QueryBundle} in the queue, then this method
   * blocks until there is one.
   * </p>
   * <p>
   * <b>Note:</b> Calls to {@link #dequeueBlock} can be mixed with calls to {@link #dequeue}.
   * </p>
   * @return The next {@link QueryBundle} in the queue.
   */
  public QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> dequeueBlock();

  public int getNumLeft();

  public void haltEnqueue();
  public void resumeEnqueue();
}
