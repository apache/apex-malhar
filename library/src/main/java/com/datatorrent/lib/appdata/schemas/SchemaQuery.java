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

import com.datatorrent.lib.appdata.qr.DataDeserializerInfo;
import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.DataValidatorInfo;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.SimpleDataDeserializer;
import com.datatorrent.lib.appdata.qr.SimpleDataValidator;

@DataType(type=SchemaQuery.TYPE)
@DataDeserializerInfo(clazz=SimpleDataDeserializer.class)
@DataValidatorInfo(clazz=SimpleDataValidator.class)
public class SchemaQuery extends Query
{
  public static final String TYPE = "schemaQuery";

  public SchemaQuery()
  {
  }
}
