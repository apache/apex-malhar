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
package org.apache.apex.malhar.lib.appdata.query.serde;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.appdata.schemas.DataQuerySnapshot;
import org.apache.apex.malhar.lib.appdata.schemas.Fields;
import org.apache.apex.malhar.lib.appdata.schemas.Message;
import org.apache.apex.malhar.lib.appdata.schemas.SchemaRegistry;
import org.apache.apex.malhar.lib.appdata.schemas.SnapshotSchema;

/**
 * This class is a validator for {@link DataQuerySnapshot} objects.
 * @since 3.0.0
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
    DataQuerySnapshot gdqt = (DataQuerySnapshot)query;
    SnapshotSchema schema = (SnapshotSchema)((SchemaRegistry)context).getSchema(gdqt.getSchemaKeys());

    Set<String> fields = schema.getValuesDescriptor().getFields().getFields();

    if (!fields.containsAll(gdqt.getFields().getFields())) {
      LOG.error("Some of the fields in the query {} are not one of the valid fields {}.", fields,
          gdqt.getFields().getFields());
      return false;
    }

    if (gdqt.getFields().getFields().isEmpty()) {
      gdqt.setFieldsVal(new Fields(fields));
    }

    return true;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataQuerySnapshotValidator.class);
}
