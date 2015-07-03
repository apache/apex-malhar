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

import java.util.Collections;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * This is a base class which represents the basic functionality of all AppData queries.
 */
public abstract class Query extends QRBase
{
  /**
   * The String that is used as a key in JSON requests to represent the shemaKeys.
   */
  public static final String FIELD_SCHEMA_KEYS = "schemaKeys";

  /**
   * The schemaKeys.
   */
  protected Map<String, String> schemaKeys;

  /**
   * No-arg constructor is required by some deserializers.
   */
  public Query()
  {
    //Do nothing
  }

  /**
   * Creates a query with the given id.
   * @param id The query id.
   */
  public Query(String id)
  {
    super(id);
  }

  /**
   * Creates a query with the given id and type.
   * @param id The query id.
   * @param type The type of the query.
   */
  public Query(String id,
               String type)
  {
    super(id, type);
  }
  /**
   * Creates a query with the given id, type, and schemaKeys.
   * @param id The query id.
   * @param type The type of the query.
   * @param schemaKeys The schemaKeys for the query.
   */
  public Query(String id,
               String type,
               Map<String, String> schemaKeys)
  {
    super(id, type);
    setSchemaKeys(schemaKeys);
  }

  /**
   * Creates a query with the given id, type, and countdown.
   * @param id The query id.
   * @param type The type of the query.
   * @param countdown The countdown for the query.
   */
  public Query(String id,
               String type,
               long countdown)
  {
    super(id, type, countdown);
  }

  /**
   * Creates a query with the given id, type, countdown, and schemaKeys.
   * @param id The query id.
   * @param type The type of the query.
   * @param countdown The countdown for the query.
   * @param schemaKeys The schemaKeys for the query.
   */
  public Query(String id,
               String type,
               long countdown,
               Map<String, String> schemaKeys)
  {
    super(id, type, countdown);
    setSchemaKeys(schemaKeys);
  }

  /**
   * Helper method to set schema keys and validate the schema keys.
   * @param schemaKeys The schemaKeys to sett and validate.
   */
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

  /**
   * Gets the schemaKeys for the query.
   * @return The schemaKeys for the query.
   */
  public Map<String, String> getSchemaKeys()
  {
    return schemaKeys;
  }
}
