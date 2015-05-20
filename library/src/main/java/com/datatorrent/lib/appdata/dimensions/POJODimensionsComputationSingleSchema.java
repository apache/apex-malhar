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
import com.datatorrent.lib.appdata.dimensions.AggregateEvent.InputAggregateEvent;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterByte;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterObject;
import com.datatorrent.lib.util.PojoUtils.GetterShort;
import com.datatorrent.lib.util.PojoUtils.GetterString;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;

public class POJODimensionsComputationSingleSchema extends AbstractDimensionsComputationSingleSchema<Object>
{
  @NotNull
  private Map<String, String> aggregateToExpression;
  @NotNull
  private Map<String, String> keyToExpression;
  private transient List<Int2ObjectMap<GPOGetters>> ddIDToAggIDToKeyGetters;
  private transient List<GPOGetters> ddIDToKeyGetters;
  private transient boolean builtGetters = false;

  public POJODimensionsComputationSingleSchema()
  {
    //Do nothing
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    /*
    for(int ddID = 0;
        ddID < eventSchema.get.size();
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
    }*/
  }

  @Override
  public InputAggregateEvent convertInputEvent(Object inputEvent, DimensionsConversionContext context)
  {
    buildGetters(inputEvent);

    return null;
  }

  private void buildGetters(Object inputEvent)
  {
    if(builtGetters) {
      return;
    }

    builtGetters = true;

    Class<?> clazz = inputEvent.getClass();


    ddIDToKeyGetters = Lists.newArrayList();
    GPOGetters gpoGetters = new GPOGetters();
    List<FieldsDescriptor> keyDescriptors = eventSchema.getDdIDToKeyDescriptor();

    for(int ddID = 0;
        ddID < eventSchema.getDdIDToKeyDescriptor().size();
        ddID++) {
      FieldsDescriptor keyDescriptor = keyDescriptors.get(ddID);

      for(int index = 0;
          index < keyDescriptor.getFieldList().size();
          index++) {
        Map<Type, List<String>> typeToFields = keyDescriptor.getTypeToFields();

        for(Map.Entry<Type, List<String>> entry: typeToFields.entrySet()) {
          Type inputType = entry.getKey();
          List<String> fields = entry.getValue();

          switch(inputType)
          {
            case BOOLEAN: {
              gpoGetters.gettersBoolean = new GetterBoolean[fields.size()];

              for(int getterIndex = 0;
                  getterIndex < fields.size();
                  getterIndex++) {
                String field = fields.get(getterIndex);
                gpoGetters.gettersBoolean[getterIndex] =
                PojoUtils.createGetterBoolean(clazz, keyToExpression.get(field));
              }

              break;
            }
            case STRING: {
              gpoGetters.gettersString = new GetterString[fields.size()];

              for(int getterIndex = 0;
                  getterIndex < fields.size();
                  getterIndex++) {
                String field = fields.get(getterIndex);
                gpoGetters.gettersString[getterIndex] =
                PojoUtils.createGetterString(clazz, keyToExpression.get(field));
              }

              break;
            }
            case CHAR: {
              gpoGetters.gettersChar = new GetterChar[fields.size()];

              for(int getterIndex = 0;
                  getterIndex < fields.size();
                  getterIndex++) {
                String field = fields.get(getterIndex);
                gpoGetters.gettersChar[getterIndex] =
                PojoUtils.createGetterChar(clazz, keyToExpression.get(field));
              }

              break;
            }
            case DOUBLE: {
              gpoGetters.gettersDouble = new GetterDouble[fields.size()];

              for(int getterIndex = 0;
                  getterIndex < fields.size();
                  getterIndex++) {
                String field = fields.get(getterIndex);
                gpoGetters.gettersDouble[getterIndex] =
                PojoUtils.createGetterDouble(clazz, keyToExpression.get(field));
              }

              break;
            }
            case FLOAT: {
              gpoGetters.gettersFloat = new GetterFloat[fields.size()];

              for(int getterIndex = 0;
                  getterIndex < fields.size();
                  getterIndex++) {
                String field = fields.get(getterIndex);
                gpoGetters.gettersFloat[getterIndex] =
                PojoUtils.createGetterFloat(clazz, keyToExpression.get(field));
              }

              break;
            }
            case LONG: {
              gpoGetters.gettersLong = new GetterLong[fields.size()];

              for(int getterIndex = 0;
                  getterIndex < fields.size();
                  getterIndex++) {
                String field = fields.get(getterIndex);
                gpoGetters.gettersLong[getterIndex] =
                PojoUtils.createExpressionGetterLong(clazz, keyToExpression.get(field));
              }

              break;
            }
            case INTEGER: {
              gpoGetters.gettersInteger = new GetterInt[fields.size()];

              for(int getterIndex = 0;
                  getterIndex < fields.size();
                  getterIndex++) {
                String field = fields.get(getterIndex);
                gpoGetters.gettersInteger[getterIndex] =
                PojoUtils.createGetterInt(clazz, keyToExpression.get(field));
              }

              break;
            }
            case SHORT: {
              gpoGetters.gettersShort = new GetterShort[fields.size()];

              for(int getterIndex = 0;
                  getterIndex < fields.size();
                  getterIndex++) {
                String field = fields.get(getterIndex);
                gpoGetters.gettersShort[getterIndex] =
                PojoUtils.createGetterShort(clazz, keyToExpression.get(field));
              }

              break;
            }
            case BYTE: {
              gpoGetters.gettersByte = new GetterByte[fields.size()];

              for(int getterIndex = 0;
                  getterIndex < fields.size();
                  getterIndex++) {
                String field = fields.get(getterIndex);
                gpoGetters.gettersByte[getterIndex] =
                PojoUtils.createGetterByte(clazz, keyToExpression.get(field));
              }

              break;
            }
            case OBJECT: {
              gpoGetters.gettersObject = new GetterObject[fields.size()];

              for(int getterIndex = 0;
                  getterIndex < fields.size();
                  getterIndex++) {
                String field = fields.get(getterIndex);
                gpoGetters.gettersObject[getterIndex] =
                PojoUtils.createGetterObject(clazz, keyToExpression.get(field));
              }

              break;
            }
            default: {
              throw new IllegalArgumentException("The type " + inputType + " is not supported.");
            }
          }
        }
      }
    }
  }

  private GPOGetters createGPOGetters(Object inputEvent)
  {
    return null;
  }

  /**
   * @return the aggregateToExpression
   */
  public Map<String, String> getAggregateToExpression()
  {
    return aggregateToExpression;
  }

  /**
   * @param aggregateToExpression the aggregateToExpression to set
   */
  public void setAggregateToExpression(Map<String, String> aggregateToExpression)
  {
    this.aggregateToExpression = aggregateToExpression;
  }

  /**
   * @return the keyToExpression
   */
  public Map<String, String> getKeyToExpression()
  {
    return keyToExpression;
  }

  /**
   * @param keyToExpression the keyToExpression to set
   */
  public void setKeyToExpression(Map<String, String> keyToExpression)
  {
    this.keyToExpression = keyToExpression;
  }

  public static class GPOGetters
  {
    public GetterBoolean[] gettersBoolean;
    public GetterChar[] gettersChar;
    public GetterByte[] gettersByte;
    public GetterShort[] gettersShort;
    public GetterInt[] gettersInteger;
    public GetterLong[] gettersLong;
    public GetterFloat[] gettersFloat;
    public GetterDouble[] gettersDouble;
    public GetterString[] gettersString;
    public GetterObject[] gettersObject;
  }
}
