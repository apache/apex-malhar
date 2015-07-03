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
package com.datatorrent.lib.appdata.schemas;

import com.google.common.base.Preconditions;

/**
 * This class holds some boilerplate for setting and getting queries from result objects. All
 * query result objects should extend this class.
 */
public abstract class Result extends QRBase
{
  /**
   * The JSON key under which data will be stored.
   */
  public static final String FIELD_DATA = "data";

  /**
   * The query which the result is a response to.
   */
  private Query query;

  /**
   * Creates a result which is a response to the given query.
   * @param query The query that this result is a response to.
   */
  public Result(Query query)
  {
    super(query.getId());
    setQuery(query);
  }

  /**
   * Creates a result which is a response to the given query,
   * and has the given countdown.
   * @param query The query that this result is a response to.
   * @param countdown The countdown for this result.
   */
  public Result(Query query,
                long countdown)
  {
    super(query.getId());
    setQuery(query);
    setCountdown(countdown);
  }

  /**
   * Helper method which sets the query.
   * @param query The query to set.
   */
  private void setQuery(Query query)
  {
    this.query = Preconditions.checkNotNull(query);
  }

  /**
   * Gets the query associated with this result.
   * @return The query associated with this result.
   */
  public Query getQuery()
  {
    return query;
  }

  @Override
  public String getId()
  {
    return query.getId();
  }
}
