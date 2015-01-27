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
package com.datatorrent.demos.dimensions.generic;

import java.util.Map;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 *
 */
public class SchemaConverter extends BaseOperator
{
  public final DefaultOutputPort<GenericEvent> output = new DefaultOutputPort<GenericEvent>();
  public final DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      output.emit(eventSchema.convertMapToGenericEvent(tuple));
    }
  };

  private String eventSchemaJSON;
  private transient EventSchema eventSchema;

  public SchemaConverter()
  {
    this.eventSchemaJSON = EventSchema.DEFAULT_SCHEMA_SALES;
    getEventSchema();
  }

  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  public final EventSchema getEventSchema()
  {
    if (eventSchema == null) {
      try {
        eventSchema = EventSchema.createFromJSON(eventSchemaJSON);
      }
      catch (Exception e) {
        throw new IllegalArgumentException("Failed to parse JSON input: " + eventSchemaJSON, e);
      }
    }
    return eventSchema;
  }

  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
    try {
      eventSchema = EventSchema.createFromJSON(eventSchemaJSON);
    }
    catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse JSON input: " + eventSchemaJSON, e);
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    setEventSchemaJSON(eventSchemaJSON);
  }

}
