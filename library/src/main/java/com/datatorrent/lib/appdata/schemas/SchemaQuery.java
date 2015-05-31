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
package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.query.serde.MessageDeserializerInfo;
import com.datatorrent.lib.appdata.query.serde.MessageType;
import com.datatorrent.lib.appdata.query.serde.MessageValidatorInfo;
import com.datatorrent.lib.appdata.query.serde.SimpleDataDeserializer;
import com.datatorrent.lib.appdata.query.serde.SimpleDataValidator;

import java.util.Map;

@MessageType(type=SchemaQuery.TYPE)
@MessageDeserializerInfo(clazz=SimpleDataDeserializer.class)
@MessageValidatorInfo(clazz=SimpleDataValidator.class)
public class SchemaQuery extends Query
{
  public static final String TYPE = "schemaQuery";

  /**
   * No-arg constructor required for deserialization.
   */
  public SchemaQuery()
  {
    //Do nothing
  }

  public SchemaQuery(String id)
  {
    super(id, TYPE);
  }

  public SchemaQuery(String id,
                     Map<String, String> schemaKeys)
  {
    super(id, TYPE, schemaKeys);
  }
}
