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

import com.datatorrent.lib.appdata.query.serde.DataDeserializerInfo;
import com.datatorrent.lib.appdata.query.serde.DataType;
import com.datatorrent.lib.appdata.query.serde.MessageValidatorInfo;
import com.datatorrent.lib.appdata.query.serde.Query;
import com.google.common.base.Preconditions;

import java.util.Map;

@DataType(type=DataQueryTabular.TYPE)
@DataDeserializerInfo(clazz=DataQueryTabularDeserializer.class)
@MessageValidatorInfo(clazz=DataQueryTabularValidator.class)
public class DataQueryTabular extends Query
{
  public static final String TYPE = "dataQuery";

  public static final String FIELD_DATA = "data";
  public static final String FIELD_FIELDS = "fields";
  public static final String SCHEMA_KEYS = "schemaKeys";

  private Fields fields;

  public DataQueryTabular()
  {
  }

  public DataQueryTabular(String id,
                          String type,
                          Fields fields)
  {
    this(id,
         type,
         fields,
         null);
  }

  public DataQueryTabular(String id,
                          String type,
                          Fields fields,
                          Map<String, String> schemaKeys)
  {
    super(id,
          type,
          schemaKeys);

    setFields(fields);
  }

  public DataQueryTabular(String id,
                          String type,
                          Fields fields,
                          long countdown)
  {
    this(id,
         type,
         fields,
         countdown,
         null);
  }

  public DataQueryTabular(String id,
                          String type,
                          Fields fields,
                          long countdown,
                          Map<String, String> schemaKeys)
  {
    super(id,
          type,
          countdown,
          schemaKeys);

    setFields(fields);
  }

  private void setFields(Fields fields)
  {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  public Fields getFields()
  {
    return fields;
  }
}
