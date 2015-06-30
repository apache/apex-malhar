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
 * This interface describes a SchemaRegistry. A schema registry is used to store and look up
 * schemas for App Data enabled operators which serve data from multiple schemas. {@link SchemaRegistry}s
 * are primarily used to create responses to {@link SchemaQuery}s.
 */
public interface SchemaRegistry
{
  /**
   * This produces the schema result for a given schema query.
   * @param schemaQuery The schema query to produce a result for.
   * @return The schema result.
   */
  public SchemaResult getSchemaResult(SchemaQuery schemaQuery);
  /**
   * Registers the given schema with this schema registry.
   * @param schema The schema to register with this registry.
   */
  public void registerSchema(Schema schema);
  /**
   * Registers the given schema with the given schema keys.
   * @param schema The schema to register.
   * @param schemaKeys The schema keys that correspond with the given schema.
   */
  public void registerSchema(Schema schema, Map<String, String> schemaKeys);
  /**
   * Gets the schema corresponding to the given schema keys.
   * @param schemaKeys The schema keys for a schema.
   * @return The schema corresponding to the given schema keys. Null if no schema was found
   * for the given schema keys.
   */
  public Schema getSchema(Map<String, String> schemaKeys);

  /**
   * Gets the number of schemas in the schema registry.
   * @return The number of schemas in the schema registry.
   */
  public int size();
}
