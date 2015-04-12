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

import com.datatorrent.lib.appdata.qr.CustomDataValidator;
import com.datatorrent.lib.appdata.qr.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class DataQueryTabularValidator implements CustomDataValidator
{
  private static final Logger logger = LoggerFactory.getLogger(DataQueryTabularValidator.class);

  public DataQueryTabularValidator()
  {
  }

  @Override
  public boolean validate(Data query, Object context)
  {
    DataQueryTabular gdqt = (DataQueryTabular) query;
    SchemaTabular schema = (SchemaTabular) context;

    Set<String> fields = schema.getValuesDescriptor().getFields().getFields();

    if(!fields.containsAll(gdqt.getFields().getFields())) {
      logger.error("Some of the fields in the query {} are not one of the valid fields {}.",
                   fields,
                   gdqt.getFields().getFields());
      return false;
    }

    return true;
  }
}
