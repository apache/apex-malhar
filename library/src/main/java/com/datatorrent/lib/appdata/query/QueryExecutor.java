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
 * This is an interface for a Query Executor which is responsible for converting a query into a result
 * that can be used by the user.
 * @param <QUERY_TYPE> The type of the query to execute.
 * @param <META_QUERY> The type of any additional meta data associated with the query when it was enqueued.
 * @param <QUEUE_CONTEXT> The type of the queue context of the query.
 * @param <RESULT> The type of the query's result.
 */
public interface QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT>
{
  /**
   * This method executes a query to produce a result.
   * @param query The query to execute.
   * @param metaQuery Any additional meta data associated with the query.
   * @param queueContext Additoinal information required to queue the query properly.
   * @return The result of the query if it's available. False otherwise.
   */
  public RESULT executeQuery(QUERY_TYPE query,
                             META_QUERY metaQuery,
                             QUEUE_CONTEXT queueContext);
}
