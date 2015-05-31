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
package com.datatorrent.lib.appdata.query.serde;

import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.DataQueryTabular;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.datatorrent.lib.appdata.schemas.TabularSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * This class is a validator for {@link DataQueryTabular} objects.
 */
public class DataQueryTabularValidator implements CustomMessageValidator
{
  /**
   * Constructor used to instantiate validator in {@link MessageDeserializerFactory}.
   */
  public DataQueryTabularValidator()
  {
  }

  @Override
  public boolean validate(Message query, Object context)
  {
    DataQueryTabular gdqt = (DataQueryTabular) query;
    TabularSchema schema = (TabularSchema) ((SchemaRegistry) context).getSchema(gdqt.getSchemaKeys());

    Set<String> fields = schema.getValuesDescriptor().getFields().getFields();

    if(!fields.containsAll(gdqt.getFields().getFields())) {
      LOG.error("Some of the fields in the query {} are not one of the valid fields {}.",
                fields,
                gdqt.getFields().getFields());
      return false;
    }

    return true;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataQueryTabularValidator.class);
}
