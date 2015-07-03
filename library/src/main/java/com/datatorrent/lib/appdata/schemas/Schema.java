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

import java.util.Map;

/**
 * This interface represents schemas such as: {@link DimensionalSchema} and {@link SnapShotSchema}.
 */
public interface Schema
{
  /**
   * Default schema ID.
   */
  public static final int DEFAULT_SCHEMA_ID = 0;

  public static final String FIELD_SCHEMA_KEYS = "schemaKeys";
  public static final String FIELD_SCHEMA = "schema";

  /**
   * The id of the schema. This is relevant for operators which support serving multiple schemas,
   * in which each schema will need a unique id.
   * @return The id of the schema.
   */
  public int getSchemaID();
  /**
   * Gets the type of the schema (e.x. point, dimensions).
   * @return The type of the schema.
   */
  public String getSchemaType();
  /**
   * Gets the version of the schema.
   * @return The version of the schema.
   */
  public String getSchemaVersion();
  /**
   * Gets the AppData json to serve in response to a schema query.
   * @return The AppData json to serve in response to a schema query.
   */
  public String getSchemaJSON();
  /**
   * Gets the schema keys which are used to send queries targeted to this schema.
   * @return The schema keys which are used to send queries targeted to this schema.
   */
  public Map<String, String> getSchemaKeys();
  /**
   * Sets the schema keys for this schema.
   * @param schemaKeys The schema keys for this schema.
   */
  public void setSchemaKeys(Map<String, String> schemaKeys);
}
