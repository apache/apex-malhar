/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.appdata.schemas;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.appdata.query.serde.MessageDeserializerInfo;
import org.apache.apex.malhar.lib.appdata.query.serde.MessageType;
import org.apache.apex.malhar.lib.appdata.query.serde.MessageValidatorInfo;
import org.apache.apex.malhar.lib.appdata.query.serde.SchemaQueryDeserializer;
import org.apache.apex.malhar.lib.appdata.query.serde.SimpleDataValidator;

/**
 * This class represents a schema query.
 * @since 3.0.0
 */
@MessageType(type = SchemaQuery.TYPE)
@MessageDeserializerInfo(clazz = SchemaQueryDeserializer.class)
@MessageValidatorInfo(clazz = SimpleDataValidator.class)
public class SchemaQuery extends Query
{
  public static final String FIELD_CONTEXT = "context";
  public static final String FIELD_CONTEXT_KEYS = "keys";

  /**
   * The type of the schemaQuery.
   */
  public static final String TYPE = "schemaQuery";

  private Map<String, String> contextKeys;

  /**
   * No-arg constructor required for deserialization.
   */
  public SchemaQuery()
  {
    //Do nothing
  }

  /**
   * Creates a schema query with the given id.
   * @param id The id of the query.
   */
  public SchemaQuery(String id)
  {
    super(id, TYPE);
  }

  /**
   * Creates a schema query with the given id, and the given schema keys.
   * @param id The id of the query.
   * @param schemaKeys The schema keys for the requested schema.
   */
  public SchemaQuery(String id, Map<String, String> schemaKeys)
  {
    super(id, TYPE, schemaKeys);
  }

  public SchemaQuery(String id, Map<String, String> schemaKeys, Map<String, String> contextKeys)
  {
    super(id, TYPE);
    this.schemaKeys = schemaKeys;
    this.contextKeys = contextKeys;
  }

  /**
   * @return the contextKeys
   */
  public Map<String, String> getContextKeys()
  {
    return contextKeys;
  }

  /**
   * @param contextKeys the contextKeys to set
   */
  public void setContextKeys(Map<String, String> contextKeys)
  {
    this.contextKeys = contextKeys;
  }

  private static final Logger LOG = LoggerFactory.getLogger(SchemaQuery.class);
}
