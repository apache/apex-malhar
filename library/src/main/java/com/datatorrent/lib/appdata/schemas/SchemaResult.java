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

import java.util.List;

import com.google.common.base.Preconditions;

import com.datatorrent.lib.appdata.query.serde.MessageSerializerInfo;
import com.datatorrent.lib.appdata.query.serde.MessageType;

/**
 * This class represents a schema result, which will be serialized into JSON and sent
 * as a result to a {@link SchemaQuery}.
 */
@MessageType(type=SchemaResult.TYPE)
@MessageSerializerInfo(clazz=SchemaResultSerializer.class)
public class SchemaResult extends Result
{
  /**
   * The type of the result.
   */
  public static final String TYPE = "schemaResult";

  /**
   * An array holding of the schemas that will be served in response to a {@link SchemaQuery}.
   */
  private Schema[] genericSchemas;

  /**
   * This constructor creates a schema result that is response to the given
   * schema query, and provides the given schemas in it's payload.
   * @param schemaQuery
   * @param genericSchemas
   */
  public SchemaResult(SchemaQuery schemaQuery,
                      Schema... genericSchemas)
  {
    super(schemaQuery);
    setGenericSchemas(genericSchemas);
  }

  /**
   * This constructor creates a schema result that is a response to the
   * given schema query, and provides the given schemas in it's payload.
   * @param schemaQuery The schema query which this schema result will be a response to.
   * @param genericSchemas The schemas to return in the schema result payload.
   */
  public SchemaResult(SchemaQuery schemaQuery,
                      List<Schema> genericSchemas)
  {
    super(schemaQuery);
    setGenericSchemas(genericSchemas);
  }

  /**
   * This is a helper method which sets and validates the schemas that will be delivered in response
   * to a schema query.
   * @param genericSchemas The schemas that are delivered in response to a schema query.
   */
  private void setGenericSchemas(List<Schema> genericSchemas)
  {
    Preconditions.checkNotNull(genericSchemas);
    Preconditions.checkArgument(!genericSchemas.isEmpty(), "Atleast one schema must be provided.");

    this.genericSchemas = genericSchemas.toArray(new Schema[0]);
  }

  /**
   * This is a helper method which sets and validates the schemas that will be delivered in
   * response to a schema query.
   * @param genericSchemas The schemas that are delivered in response to a schema query.
   */
  private void setGenericSchemas(Schema... genericSchemas)
  {
    Preconditions.checkNotNull(genericSchemas);
    Preconditions.checkArgument(genericSchemas.length > 0, "Atleast one schema must be provided.");

    this.genericSchemas = genericSchemas;
  }

  /**
   * Gets the schemas which will be delivered in response to the schema query.
   * @return The schemas which will be delivered in response to the schema query.
   */
  public Schema[] getGenericSchemas()
  {
    return genericSchemas;
  }
}
