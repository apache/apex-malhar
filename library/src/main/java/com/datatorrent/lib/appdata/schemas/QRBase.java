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

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

/**
 * This is the base class used to represent AppData queries. App Data quries have the following features:
 * <br/>
 * <br/>
 * <ul>
 *  <li><b>id:</b>This is the client id for the query. It is also the same id which is attached to results.</li>
 *  <li><b>countdown:</b>This is the number of times results are returned for the query. This is meant to allow
 *  the client to display updated data without incurring the latency of issuing a separate query for each update.</li>
 *  <li><b>schemaKeys:</b>This is used in the case where an operator responding to queries is hosting multiple schemas. When
 *  an operator is exposing multiple schemas, the schemakeys are used to select the schema that this query is intended to be
 *  run against.</li>
 * </ul>
 *
 * This base class holds the basic setters and getters for manipulating and storing these query properies.
 */
public abstract class QRBase extends Message
{
  /**
   * The String that is used as a key in JSON requests to represent the countdown.
   */
  public static final String FIELD_COUNTDOWN = "countdown";
  /**
   * The String that is used as a key in JSON requests to represent the query id.
   */
  public static final String FIELD_ID = "id";

  /**
   * The query id.
   */
  @NotNull
  private String id;
  /**
   * The query countdown.
   */
  private long countdown;

  /**
   * Creates a query base object with nothing set.
   */
  public QRBase()
  {
  }

  /**
   * Creates a query with the given id.
   * @param id The query id.
   */
  public QRBase(String id)
  {
    this.id = Preconditions.checkNotNull(id);
  }

  /**
   * Creates a query with the given id and type.
   * @param id The query id.
   * @param type The type of the query.
   */
  public QRBase(String id,
                String type)
  {
    super(type);
    this.id = Preconditions.checkNotNull(id);
  }

  /**
   * Creates a query with the given id, type, and countdown.
   * @param id The query id.
   * @param type The type of the query.
   * @param countdown The countdown for the query.
   */
  public QRBase(String id,
                String type,
                long countdown)
  {
    this(id, type);
    setCountdown(countdown);
  }


  /**
   * Sets the countdown for the query. Valid countdowns are greater than zero.
   * @param countdown The countdown for the query.
   */
  public final void setCountdown(long countdown)
  {
    Preconditions.checkArgument(countdown > 0L);
    this.countdown = countdown;
  }

  /**
   * Gets the countdown for the query.
   * @return The countdown for the query.
   */
  public long getCountdown()
  {
    return countdown;
  }

  /**
   * Returns true if the countdown is one. False otherwise.
   * @return True if the countdown is one. False otherwise.
   */
  public boolean isOneTime()
  {
    return countdown == 0L;
  }

  /**
   * Returns the query id.
   * @return The query id.
   */
  public String getId()
  {
    return id;
  }

  /**
   * Sets the query id.
   * @param id The query id to set.
   */
  public void setId(String id)
  {
    this.id = id;
  }

  @Override
  public String toString()
  {
    return "Query{" + "id=" + id + ", type=" + getType() + '}';
  }
}
