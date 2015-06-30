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

/**
 * This is a container object to be used by Queue Managers. It is a container for a query, the query's meta data,
 * and the query's queueing context.
 * @param <QUERY_TYPE> The type of the query.
 * @param <META_QUERY> The type of any meta data to be included with the query.
 * @param <QUEUE_CONTEXT> The context associated with the query when it was enqueued.
 */
public class QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>
{
  protected QUERY_TYPE query;
  protected META_QUERY metaQuery;
  protected QUEUE_CONTEXT queueContext;

  public QueryBundle()
  {
  }

  public QueryBundle(QUERY_TYPE query,
                     META_QUERY metaQuery,
                     QUEUE_CONTEXT queueContext)
  {
    this.query = query;
    this.metaQuery = metaQuery;
    this.queueContext = queueContext;
  }

  public QUERY_TYPE getQuery()
  {
    return query;
  }

  public META_QUERY getMetaQuery()
  {
    return metaQuery;
  }

  public QUEUE_CONTEXT getQueueContext()
  {
    return queueContext;
  }
}
