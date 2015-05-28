/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.appdata.query.serde;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import javax.validation.constraints.NotNull;

import java.util.Collections;
import java.util.Map;

public class Query extends Message
{
  public static final String FIELD_ID = "id";

  public static final String FIELD_SCHEMA_KEYS = "schemaKeys";
  public static final String FIELD_COUNTDOWN = "countdown";

  @NotNull
  private String id;
  private long countdown;

  private Map<String, String> schemaKeys;

  public Query()
  {
  }

  public Query(String id)
  {
    super(id);
    this.id = Preconditions.checkNotNull(id);
  }

  public Query(String id,
               String type)
  {
    super(type);
    this.id = Preconditions.checkNotNull(id);
  }

  public Query(String id,
               String type,
               Map<String, String> schemaKeys)
  {
    super(type);
    Preconditions.checkNotNull(id);
    this.id = id;
    setSchemaKeys(schemaKeys);
  }

  public Query(String id,
               String type,
               long countdown)
  {
    this(id, type);
    setCountdown(countdown);
  }

  public Query(String id,
               String type,
               long countdown,
               Map<String, String> schemaKeys)
  {
    this(id, type);
    setCountdown(countdown);
    setSchemaKeys(schemaKeys);
  }

  private void setSchemaKeys(Map<String, String> schemaKeys)
  {
    if(schemaKeys == null) {
      return;
    }

    for(Map.Entry<String, String> entry: schemaKeys.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.schemaKeys = Collections.unmodifiableMap(Maps.newHashMap(schemaKeys));
  }

  public Map<String, String> getSchemaKeys()
  {
    return schemaKeys;
  }

  public final void setCountdown(long countdown)
  {
    Preconditions.checkArgument(countdown > 0L);
    this.countdown = countdown;
  }

  public long getCountdown()
  {
    return countdown;
  }

  public boolean isOneTime()
  {
    return countdown <= 0L;
  }

  /**
   * @return the id
   */
  public String getId()
  {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(String id)
  {
    this.id = id;
  }

  public boolean queueEquals(Query query)
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "Query{" + "id=" + id + ", type=" + getType() + '}';
  }
}
