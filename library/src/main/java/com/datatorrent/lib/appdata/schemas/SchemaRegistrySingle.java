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

import java.io.Serializable;

import java.util.Map;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This schema registry holds a single schema. It is intended to be used in operators
 * which serve a single schema.
 */
public class SchemaRegistrySingle implements SchemaRegistry, Serializable
{
  private static final long serialVersionUID = 20150513928L;
  private static final Logger logger = LoggerFactory.getLogger(SchemaRegistrySingle.class);

  /**
   * The schema held by this schema registry.
   */
  private Schema schema;

  /**
   * Creates a schema registry whose schema will be registered later.
   */
  public SchemaRegistrySingle()
  {
  }

  /**
   * Creates a schema registry with the given schema.
   * @param schema
   */
  public SchemaRegistrySingle(Schema schema)
  {
    setSchema(schema);
  }

  private void setSchema(Schema schema)
  {
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(schema.getSchemaKeys() == null,
                                "The provided schema should not have schema keys "
                                + schema.getSchemaKeys()
                                + " since they will never be used.");

    this.schema = schema;
  }

  @Override
  public SchemaResult getSchemaResult(SchemaQuery schemaQuery)
  {
    Preconditions.checkNotNull(schemaQuery, "This should never happen.");

    if(schemaQuery.getSchemaKeys() != null) {
      logger.error("Schema keys in the given query don't apply for single schema registry: schemaKeys={}", schemaQuery.getSchemaKeys());
      return null;
    }

    return new SchemaResult(schemaQuery, schema);
  }

  @Override
  public void registerSchema(Schema schema)
  {
    Preconditions.checkState(this.schema == null, "A schema is already set.");
    this.schema = schema;
  }

  @Override
  public Schema getSchema(Map<String, String> schemaKeys)
  {
    return schema;
  }

  @Override
  public void registerSchema(Schema schema, Map<String, String> schemaKeys)
  {
    throw new UnsupportedOperationException("Schema keys are not supported in SchemaRegistrySingle.");
  }

  @Override
  public int size()
  {
    return schema == null ? 0: 1;
  }
}
