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

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.SchemaTabular;
import com.datatorrent.lib.converter.Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class TabularMapConverter implements Converter<Map<String, Object>, GPOMutable, SchemaTabular>
{
  private static final Logger logger = LoggerFactory.getLogger(TabularMapConverter.class);

  private Map<String, String> tableFieldToMapField;

  public TabularMapConverter()
  {
  }

  @Override
  public GPOMutable convert(Map<String, Object> inputEvent, SchemaTabular context)
  {
    logger.info("input event {}", inputEvent);

    FieldsDescriptor fd = context.getValuesDescriptor();
    GPOMutable values = new GPOMutable(fd);

    List<String> fields = fd.getFieldList();

    for(int index = 0;
        index < fields.size();
        index++) {
      String field = fields.get(index);
      values.setField(field, inputEvent.get(getMapField(field)));
      logger.info("{} {} {}", field, getMapField(field), values.getField(field));
    }

    return values;
  }

  private String getMapField(String field)
  {
    if(tableFieldToMapField == null) {
      return field;
    }

    String mapField = tableFieldToMapField.get(field);

    if(mapField == null) {
      return field;
    }

    return mapField;
  }

  /**
   * @return the tableFieldToMapField
   */
  public Map<String, String> getTableFieldToMapField()
  {
    return tableFieldToMapField;
  }

  /**
   * @param tableFieldToMapField the tableFieldToMapField to set
   */
  public void setTableFieldToMapField(@NotNull Map<String, String> tableFieldToMapField)
  {
    Preconditions.checkNotNull(tableFieldToMapField);

    for(Map.Entry<String, String> entry: tableFieldToMapField.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.tableFieldToMapField = Maps.newHashMap(tableFieldToMapField);
  }
}
