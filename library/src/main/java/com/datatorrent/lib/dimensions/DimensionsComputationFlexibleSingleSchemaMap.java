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

package com.datatorrent.lib.dimensions;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;

import java.util.Map;

public class DimensionsComputationFlexibleSingleSchemaMap extends AbstractDimensionsComputationFlexibleSingleSchema<Map<String, Object>>
{
  private Map<String, String> fieldToMapField;

  public DimensionsComputationFlexibleSingleSchemaMap()
  {
  }

  @Override
  public InputEvent convertInput(Map<String, Object> input, DimensionsConversionContext conversionContext)
  {
    FieldsDescriptor keyFieldsDescriptor = conversionContext.keyFieldsDescriptor;
    FieldsDescriptor valueFieldsDescriptor = conversionContext.aggregateDescriptor;

    GPOMutable keyMutable = new GPOMutable(keyFieldsDescriptor);

    for(int index = 0;
        index < keyFieldsDescriptor.getFieldList().size();
        index++) {
      String field = keyFieldsDescriptor.getFieldList().get(index);

      if(field.equals(DimensionsDescriptor.DIMENSION_TIME)) {
        long time = (Long) input.get(DimensionsDescriptor.DIMENSION_TIME);
        keyMutable.setField(DimensionsDescriptor.DIMENSION_TIME,
                            conversionContext.dd.getTimeBucket().roundDown(time));
      }
      else if(field.equals(DimensionsDescriptor.DIMENSION_TIME_BUCKET)) {
        keyMutable.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET,
                            conversionContext.dd.getTimeBucket().ordinal());
      }
      else {
        keyMutable.setField(field, input.get(getMapField(field)));
      }
    }

    GPOMutable valueMutable = new GPOMutable(valueFieldsDescriptor);

    for(int index = 0;
        index < valueFieldsDescriptor.getFieldList().size();
        index++) {
      String field = valueFieldsDescriptor.getFieldList().get(index);
      valueMutable.setField(field, input.get(getMapField(field)));
    }

    return new InputEvent(new EventKey(conversionContext.schemaID,
                                       conversionContext.dimensionDescriptorID,
                                       conversionContext.aggregatorID,
                                       keyMutable),
                          valueMutable);
  }

  private String getMapField(String field)
  {
    if(fieldToMapField == null) {
      return field;
    }

    String mapField = fieldToMapField.get(field);

    if(mapField == null) {
      return field;
    }

    return mapField;
  }

  /**
   * @return the fieldToMapField
   */
  public Map<String, String> getFieldToMapField()
  {
    return fieldToMapField;
  }

  /**
   * @param fieldToMapField the fieldToMapField to set
   */
  public void setFieldToMapField(Map<String, String> fieldToMapField)
  {
    this.fieldToMapField = fieldToMapField;
  }
}
