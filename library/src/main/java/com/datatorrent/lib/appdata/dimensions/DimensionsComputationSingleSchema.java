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

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.schemas.DimensionalEventSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class DimensionsComputationSingleSchema<INPUT_EVENT> extends DimensionsComputation<INPUT_EVENT>
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsComputationSingleSchema.class);
  public static final int DEFAULT_SCHEMA_ID = 1;

  @NotNull
  private String eventSchemaJSON;
  protected DimensionalEventSchema eventSchema;
  private transient DimensionsConversionContext conversionContext = new DimensionsConversionContext();
  private int schemaID = DEFAULT_SCHEMA_ID;

  public DimensionsComputationSingleSchema()
  {
    this.aggregatorInfo = AggregatorUtils.DEFAULT_AGGREGATOR_INFO;
  }

  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = Preconditions.checkNotNull(eventSchemaJSON, "eventSchemaJSON");
  }

  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    if(eventSchema == null) {
      eventSchema = new DimensionalEventSchema(eventSchemaJSON,
                                              getAggregatorInfo());
    }

    logger.debug("Event Schema {}", eventSchema);
  }

  @Override
  public void convertInputEvent(INPUT_EVENT inputEvent, List<AggregateEvent> aggregateEventBuffer)
  {
    List<FieldsDescriptor> keyFieldsDescriptors = eventSchema.getDdIDToKeyDescriptor();

    for(int ddID = 0;
        ddID < keyFieldsDescriptors.size();
        ddID++) {
      FieldsDescriptor keyFieldsDescriptor = keyFieldsDescriptors.get(ddID);
      Int2ObjectMap<FieldsDescriptor> map = eventSchema.getDdIDToAggIDToInputAggDescriptor().get(ddID);
      IntArrayList aggIDList = eventSchema.getDdIDToAggIDs().get(ddID);

      for(int aggIDIndex = 0;
          aggIDIndex < aggIDList.size();
          aggIDIndex++) {
        int aggID = aggIDList.get(aggIDIndex);

        conversionContext.schemaID = schemaID;
        conversionContext.dimensionDescriptorID = ddID;
        conversionContext.aggregatorID = aggID;

        conversionContext.dd = eventSchema.getDdIDToDD().get(ddID);
        conversionContext.keyFieldsDescriptor = keyFieldsDescriptor;
        conversionContext.aggregateDescriptor = map.get(aggID);

        aggregateEventBuffer.add(createGenericAggregateEvent(inputEvent,
                                                             conversionContext));
      }
    }
  }

  @Override
  public FieldsDescriptor getAggregateFieldsDescriptor(int schemaID, int dimensionDescriptorID, int aggregatorID)
  {
    return eventSchema.getDdIDToAggIDToOutputAggDescriptor().get(dimensionDescriptorID).get(aggregatorID);
  }

  public abstract AggregateEvent createGenericAggregateEvent(INPUT_EVENT inputEvent,
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
}
