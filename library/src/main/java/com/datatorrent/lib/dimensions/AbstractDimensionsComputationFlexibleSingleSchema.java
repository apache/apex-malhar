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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import javax.validation.constraints.NotNull;

import java.util.List;

public abstract class AbstractDimensionsComputationFlexibleSingleSchema<INPUT> extends
                      AbstractDimensionsComputationFlexible<INPUT>
{
  public static final int DEFAULT_SCHEMA_ID = 1;

  @NotNull
  private String eventSchemaJSON;
  protected DimensionalConfigurationSchema eventSchema;

  private transient final DimensionsConversionContext conversionContext = new DimensionsConversionContext();
  private int schemaID = DEFAULT_SCHEMA_ID;

  public AbstractDimensionsComputationFlexibleSingleSchema()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    if(eventSchema == null) {
      eventSchema = new DimensionalConfigurationSchema(eventSchemaJSON,
                                                       aggregatorRegistry);
    }
  }

  @Override
  public void processInputEvent(INPUT input)
  {
    List<FieldsDescriptor> keyFieldsDescriptors = eventSchema.getDdIDToKeyDescriptor();

    for(int ddID = 0;
        ddID < keyFieldsDescriptors.size();
        ddID++) {
      FieldsDescriptor keyFieldsDescriptor = keyFieldsDescriptors.get(ddID);
      Int2ObjectMap<FieldsDescriptor> map = eventSchema.getDdIDToAggIDToInputAggDescriptor().get(ddID);
      IntArrayList aggIDList = eventSchema.getDdIDToAggIDs().get(ddID);
      DimensionsDescriptor dd = eventSchema.getDdIDToDD().get(ddID);

      for(int aggIDIndex = 0;
          aggIDIndex < aggIDList.size();
          aggIDIndex++) {
        int aggID = aggIDList.get(aggIDIndex);

        conversionContext.schemaID = schemaID;
        conversionContext.dimensionDescriptorID = ddID;
        conversionContext.aggregatorID = aggID;
        conversionContext.ddID = ddID;

        conversionContext.dd = dd;
        conversionContext.keyFieldsDescriptor = keyFieldsDescriptor;
        conversionContext.aggregateDescriptor = map.get(aggID);

        InputEvent inputE = convertInput(input,
                                         conversionContext);

        int aggregateIndex = this.aggregatorIdToAggregateIndex.get(conversionContext.aggregatorID);
        this.maps[aggregateIndex].aggregate(inputE);
      }
    }
  }

  @Override
  public abstract InputEvent convertInput(INPUT input,
                                          DimensionsConversionContext conversionContext);

  /**
   * @return the schemaID
   */
  public int getSchemaID()
  {
    return schemaID;
  }

  /**
   * @param schemaID the schemaID to set
   */
  public void setSchemaID(int schemaID)
  {
    this.schemaID = schemaID;
  }

  /**
   * @return the eventSchemaJSON
   */
  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  /**
   * @param eventSchemaJSON the eventSchemaJSON to set
   */
  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
  }
}
