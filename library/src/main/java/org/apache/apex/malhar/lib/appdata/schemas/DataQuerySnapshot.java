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

import org.apache.apex.malhar.lib.appdata.query.serde.DataQuerySnapshotDeserializer;
import org.apache.apex.malhar.lib.appdata.query.serde.DataQuerySnapshotValidator;
import org.apache.apex.malhar.lib.appdata.query.serde.MessageDeserializerInfo;
import org.apache.apex.malhar.lib.appdata.query.serde.MessageType;
import org.apache.apex.malhar.lib.appdata.query.serde.MessageValidatorInfo;

import com.google.common.base.Preconditions;

/**
 * This object represents queries issued against the {@link SnapshotSchema}.
 * @since 3.0.0
 */
@MessageType(type = DataQuerySnapshot.TYPE)
@MessageDeserializerInfo(clazz = DataQuerySnapshotDeserializer.class)
@MessageValidatorInfo(clazz = DataQuerySnapshotValidator.class)
public class DataQuerySnapshot extends Query
{
  /**
   * The type of this query.
   */
  public static final String TYPE = "dataQuery";

  /**
   * The JSON key string for the data payload of the query.
   */
  public static final String FIELD_DATA = "data";
  /**
   * The JSON key string for the fields requested in the query.
   */
  public static final String FIELD_FIELDS = "fields";
  /**
   * The JSON string for the schemaKeys in the query.
   */
  public static final String SCHEMA_KEYS = "schemaKeys";
  /**
   * The JSON string for the incompleteResultOK field in the query.
   */
  public static final String FIELD_INCOMPLETE_RESULTS_OK = "incompleteResultOK";

  /**
   * The fields requested to be returned in the query.
   */
  private Fields fields;

  /**
   * This creates a query with the given id, which requests the given fields. This constructor
   * assumes that the query is "one time", and that it is issued against a single schema operator.
   * @param id The id of the query.
   * @param fields The fields requested by the query.
   */
  public DataQuerySnapshot(String id, Fields fields)
  {
    this(id, fields, null);
  }

  /**
   * This creates a query with the given id, which requests the given fields,
   * and is applied against the schema with the given schema keys. This constructor
   * assumes that the query is "one time".
   * @param id The id of the query.
   * @param fields The requested fields in the query.
   * @param schemaKeys The schema keys corresponding to the schema this query will be directed against.
   */
  public DataQuerySnapshot(String id, Fields fields, Map<String, String> schemaKeys)
  {
    super(id, TYPE, schemaKeys);

    setFields(fields);
  }

  /**
   * This creates a query with the given id, fields, and countdown. This constructor assumes
   * that the query is issued against a single schema operator.
   * @param id The id of the query.
   * @param fields The requested fields in the query.
   * @param countdown The countdown for the query.
   */
  public DataQuerySnapshot(String id, Fields fields, long countdown)
  {
    this(id, fields, countdown, null);
  }

  /**
   * This creates a query with the given id, fields, countdown, and schema keys.
   * @param id The id of the query.
   * @param fields The requested fields in the query.
   * @param countdown The countdown for the query.
   * @param schemaKeys The schemaKeys which identify the schema which the query is
   * issued against.
   */
  public DataQuerySnapshot(String id, Fields fields, long countdown, Map<String, String> schemaKeys)
  {
    super(id, TYPE, countdown, schemaKeys);

    setFields(fields);
  }

  /**
   * Sets the fields of the query.
   * @param fields The fields of the query.
   */
  private void setFields(Fields fields)
  {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  public void setFieldsVal(Fields fields)
  {
    setFields(fields);
  }

  /**
   * Gets the fields of the query.
   * @return The fields of the query.
   */
  public Fields getFields()
  {
    return fields;
  }
}
