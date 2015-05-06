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

import com.datatorrent.lib.appbuilder.convert.pojo.PojoFieldRetriever;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.converter.Converter;
import com.google.common.base.Preconditions;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DimensionsPOJOConverter implements Converter<Object, AggregateEvent, DimensionsConversionContext>
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsPOJOConverter.class);

  @NotNull
  private PojoFieldRetriever pojoFieldRetriever;

  public DimensionsPOJOConverter()
  {
  }

  @Override
  public AggregateEvent convert(Object inputEvent, DimensionsConversionContext context)
  {
    GPOMutable key = new GPOMutable(context.keyFieldsDescriptor);

    List<String> fields = key.getFieldDescriptor().getFields().getFieldsList();

    for(int fieldIndex = 0;
        fieldIndex < fields.size();
        fieldIndex++) {
      String field = fields.get(fieldIndex);
      Type type = key.getFieldDescriptor().getType(field);
      if(field.equals(DimensionsDescriptor.DIMENSION_TIME_BUCKET)) {
        key.setField(field, context.dd.getTimeBucket().ordinal());
      }
      else if(field.equals(DimensionsDescriptor.DIMENSION_TIME)) {
        long timestamp = pojoFieldRetriever.getLong(field, inputEvent);
        timestamp = context.dd.getTimeBucket().roundDown(timestamp);
        key.setField(field, timestamp);
      }
      else {
        switch(type) {
          case BOOLEAN:
          {
            key.setField(field, pojoFieldRetriever.getBoolean(field, inputEvent));
            break;
          }
          case CHAR:
          {
            key.setField(field, pojoFieldRetriever.getChar(field, inputEvent));
            break;
          }
          case STRING:
          {
            key.setField(field, pojoFieldRetriever.getString(field, inputEvent));
            break;
          }
          case BYTE:
          {
            key.setField(field, pojoFieldRetriever.getByte(field, inputEvent));
            break;
          }
          case SHORT:
          {
            key.setField(field, pojoFieldRetriever.getShort(field, inputEvent));
            break;
          }
          case INTEGER:
          {
            key.setField(field, pojoFieldRetriever.getInt(field, inputEvent));
            break;
          }
          case LONG:
          {
            key.setField(field, pojoFieldRetriever.getLong(field, inputEvent));
            break;
          }
          case FLOAT:
          {
            key.setField(field, pojoFieldRetriever.getFloat(field, inputEvent));
            break;
          }
          case DOUBLE:
          {
            key.setField(field, pojoFieldRetriever.getDouble(field, inputEvent));
            break;
          }
          case OBJECT:
          {
            key.setField(field, pojoFieldRetriever.getObject(field, inputEvent));
            break;
          }
          default:
            throw new UnsupportedOperationException("The type " + type + " is not supported.");
        }
      }
    }

    GPOMutable aggregates = new GPOMutable(context.aggregateDescriptor);

    fields = aggregates.getFieldDescriptor().getFields().getFieldsList();

    for(int fieldIndex = 0;
        fieldIndex < fields.size();
        fieldIndex++) {
      String field = fields.get(fieldIndex);
      Type type = aggregates.getFieldDescriptor().getType(field);
      aggregates.setField(field, pojoFieldRetriever.get(field, inputEvent));

        switch(type) {
          case BOOLEAN:
          {
            aggregates.setField(field, pojoFieldRetriever.getBoolean(field, inputEvent));
            break;
          }
          case CHAR:
          {
            aggregates.setField(field, pojoFieldRetriever.getChar(field, inputEvent));
            break;
          }
          case STRING:
          {
            aggregates.setField(field, pojoFieldRetriever.getString(field, inputEvent));
            break;
          }
          case BYTE:
          {
            aggregates.setField(field, pojoFieldRetriever.getByte(field, inputEvent));
            break;
          }
          case SHORT:
          {
            aggregates.setField(field, pojoFieldRetriever.getShort(field, inputEvent));
            break;
          }
          case INTEGER:
          {
            aggregates.setField(field, pojoFieldRetriever.getInt(field, inputEvent));
            break;
          }
          case LONG:
          {
            aggregates.setField(field, pojoFieldRetriever.getLong(field, inputEvent));
            break;
          }
          case FLOAT:
          {
            aggregates.setField(field, pojoFieldRetriever.getFloat(field, inputEvent));
            break;
          }
          case DOUBLE:
          {
            aggregates.setField(field, pojoFieldRetriever.getDouble(field, inputEvent));
            break;
          }
          case OBJECT:
          {
            aggregates.setField(field, pojoFieldRetriever.getObject(field, inputEvent));
            break;
          }
          default:
            throw new UnsupportedOperationException("The type " + type + " is not supported.");
        }
    }

    return new AggregateEvent(key,
                              aggregates,
                              context.schemaID,
                              context.dimensionDescriptorID,
                              context.aggregatorID);
  }

  /**
   * @return the pojoFieldRetriever
   */
  public PojoFieldRetriever getPojoFieldRetriever()
  {
    return pojoFieldRetriever;
  }

  /**
   * @param pojoFieldRetriever the pojoFieldRetriever to set
   */
  public void setPojoFieldRetriever(@NotNull PojoFieldRetriever pojoFieldRetriever)
  {
    this.pojoFieldRetriever = Preconditions.checkNotNull(pojoFieldRetriever);
  }
}
