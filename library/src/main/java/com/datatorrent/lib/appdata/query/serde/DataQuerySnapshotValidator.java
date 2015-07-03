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
package com.datatorrent.lib.appdata.query.serde;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.schemas.DataQuerySnapshot;
import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.datatorrent.lib.appdata.schemas.SnapshotSchema;

/**
 * This class is a validator for {@link DataQuerySnapshot} objects.
 */
public class DataQuerySnapshotValidator implements CustomMessageValidator
{
  /**
   * Constructor used to instantiate validator in {@link MessageDeserializerFactory}.
   */
  public DataQuerySnapshotValidator()
  {
  }

  @Override
  public boolean validate(Message query, Object context)
  {
    DataQuerySnapshot gdqt = (DataQuerySnapshot) query;
    SnapshotSchema schema = (SnapshotSchema) ((SchemaRegistry) context).getSchema(gdqt.getSchemaKeys());

    Set<String> fields = schema.getValuesDescriptor().getFields().getFields();

    if(!fields.containsAll(gdqt.getFields().getFields())) {
      LOG.error("Some of the fields in the query {} are not one of the valid fields {}.",
                fields,
                gdqt.getFields().getFields());
      return false;
    }

    return true;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataQuerySnapshotValidator.class);
}
