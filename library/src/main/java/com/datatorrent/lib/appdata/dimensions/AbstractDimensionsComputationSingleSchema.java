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
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent.InputAggregateEvent;
import com.datatorrent.lib.appdata.schemas.DimensionalEventSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import javax.validation.constraints.NotNull;

import java.util.List;

public abstract class AbstractDimensionsComputationSingleSchema<INPUT_EVENT> extends com.datatorrent.lib.statistics.DimensionsComputation<InputAggregateEvent, AggregateEvent>
{
  public static final int DEFAULT_SCHEMA_ID = 1;

  @NotNull
  private AggregatorInfo aggregatorInfo;
  private transient List<InputAggregateEvent> aggregateEventBuffer = Lists.newArrayList();
  @NotNull
  private String eventSchemaJSON;
  protected DimensionalEventSchema eventSchema;
  private transient DimensionsConversionContext conversionContext = new DimensionsConversionContext();
  private int schemaID = DEFAULT_SCHEMA_ID;

  public AbstractDimensionsComputationSingleSchema()
  {
    //Do nothing
  }

  @Override
  public void setup(OperatorContext context)
  {
    if(eventSchema == null) {
      eventSchema = new DimensionalEventSchema(eventSchemaJSON,
                                              getAggregatorInfo());
    }

    aggregatorInfo.setup();

    List<DimensionsComputationAggregator> aggregatorList = Lists.newArrayList();

    for(int ddID = 0;
        ddID < eventSchema.getDdIDToAggIDToOutputAggDescriptor().size();
        ddID++) {
      Int2ObjectMap<FieldsDescriptor> aggIDToFieldDescriptorMap =
      eventSchema.getDdIDToAggIDToOutputAggDescriptor().get(ddID);

      for(int aggIDIndex = 0;
          aggIDIndex < aggIDToFieldDescriptorMap.size();
          aggIDIndex++) {
        FieldsDescriptor outputDescriptor = aggIDToFieldDescriptorMap.get(aggIDIndex);
        aggregatorList.add(
        new DimensionsComputationAggregator(aggregatorInfo.getStaticAggregatorIDToAggregator().get(aggIDIndex),
                                            outputDescriptor));
      }
    }

    this.setAggregators(aggregatorList.toArray(new DimensionsComputationAggregator[0]));
    super.setup(context);
  }

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<INPUT_EVENT> rawInput = new DefaultInputPort<INPUT_EVENT>() {
    @Override
    public void process(INPUT_EVENT tuple)
    {
      processRawInput(tuple);
    }
  };

  public void processRawInput(INPUT_EVENT inputEvent)
  {
    List<FieldsDescriptor> keyFieldsDescriptors = eventSchema.getDdIDToKeyDescriptor();

    int aggregatorIndex = 0;

    for(int ddID = 0;
        ddID < keyFieldsDescriptors.size();
        ddID++) {
      FieldsDescriptor keyFieldsDescriptor = keyFieldsDescriptors.get(ddID);
      Int2ObjectMap<FieldsDescriptor> map = eventSchema.getDdIDToAggIDToInputAggDescriptor().get(ddID);
      IntArrayList aggIDList = eventSchema.getDdIDToAggIDs().get(ddID);

      for(int aggIDIndex = 0;
          aggIDIndex < aggIDList.size();
          aggIDIndex++, aggregatorIndex++) {
        int aggID = aggIDList.get(aggIDIndex);

        conversionContext.schemaID = schemaID;
        conversionContext.dimensionDescriptorID = ddID;
        conversionContext.aggregatorID = aggID;

        conversionContext.dd = eventSchema.getDdIDToDD().get(ddID);
        conversionContext.keyFieldsDescriptor = keyFieldsDescriptor;
        conversionContext.aggregateDescriptor = map.get(aggID);

        InputAggregateEvent inputAggregateEvent =
        convertInputEvent(inputEvent, conversionContext);

        if(aggregationWindowCount == 0) {
          output.emit(
          aggregatorMaps[aggregatorIndex].aggregator.getGroup(inputAggregateEvent, aggregatorIndex));
        }
        else {
          aggregatorMaps[aggregatorIndex].add(inputAggregateEvent, aggregatorIndex);
        }
      }
    }
  }

  @Override
  public void processInputTuple(InputAggregateEvent inputAggregateEvent) {
    throw new UnsupportedOperationException("The data input port should not be connected on this operator.");
  }

  public abstract InputAggregateEvent convertInputEvent(INPUT_EVENT inputEvent, DimensionsConversionContext context);

  @Override
  public void setAggregators(Aggregator<InputAggregateEvent, com.datatorrent.lib.appdata.dimensions.AggregateEvent>[] aggregators)
  {
    throw new UnsupportedOperationException("This method is not supported.");
  }

  @Override
  public Aggregator<InputAggregateEvent, com.datatorrent.lib.appdata.dimensions.AggregateEvent>[] getAggregators()
  {
    throw new UnsupportedOperationException("This method is not supported.");
  }

  /**
   * @return the aggregatorInfo
   */
  public AggregatorInfo getAggregatorInfo()
  {
    return aggregatorInfo;
  }

  /**
   * @param aggregatorInfo the aggregatorInfo to set
   */
  public void setAggregatorInfo(@NotNull AggregatorInfo aggregatorInfo)
  {
    this.aggregatorInfo = aggregatorInfo;
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
