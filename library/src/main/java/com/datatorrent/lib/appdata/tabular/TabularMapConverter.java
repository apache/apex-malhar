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

package com.datatorrent.lib.appdata.tabular;

import com.datatorrent.lib.appdata.converter.MapGPOConverterSchema;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaTabular;
import com.datatorrent.lib.converter.Converter;

import java.util.List;
import java.util.Map;

public class TabularMapConverter implements Converter<Map<String, Object>, GPOMutable, SchemaTabular>
{
  private MapGPOConverterSchema converterSchema;

  public TabularMapConverter()
  {
  }

  public void setConversionSchema(String conversionSchema)
  {
    converterSchema = new MapGPOConverterSchema(conversionSchema);
  }

  public MapGPOConverterSchema getConversionSchema()
  {
    return converterSchema;
  }

  @Override
  public GPOMutable convert(Map<String, Object> inputEvent, SchemaTabular context)
  {
    FieldsDescriptor fd = context.getValuesDescriptor();
    GPOMutable values = new GPOMutable(fd);

    List<String> fields = fd.getFieldList();

    for(int index = 0;
        index < fields.size();
        index++) {
      String field = fields.get(index);
      values.setField(field, inputEvent.get(converterSchema.getMapField(field)));
    }

    return values;
  }
}
