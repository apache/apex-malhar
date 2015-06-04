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

import com.datatorrent.lib.appdata.gpo.GPOGetters;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class DimensionsComputationFlexibleSingleSchemaPOJO extends AbstractDimensionsComputationFlexibleSingleSchema<Object>
{
  @NotNull
  private Map<String, String> aggregateToExpression;
  @NotNull
  private Map<String, String> keyToExpression;

  private transient List<Int2ObjectMap<GPOGetters>> ddIDToAggIDToGetters;
  private transient List<GPOGetters> ddIDToKeyGetters;

  private transient boolean builtGetters = false;

  public DimensionsComputationFlexibleSingleSchemaPOJO()
  {
  }

  @Override
  public InputEvent convertInput(Object input,
                                 DimensionsConversionContext conversionContext)
  {
    buildGetters(input);

    GPOMutable key = new GPOMutable(conversionContext.keyFieldsDescriptor);
    GPOGetters keyGetters =
    ddIDToKeyGetters.get(conversionContext.ddID);
    GPOUtils.copyPOJOToGPO(key, keyGetters, input);

    GPOMutable aggregates = new GPOMutable(conversionContext.aggregateDescriptor);
    GPOGetters aggregateGetters =
    ddIDToAggIDToGetters.get(conversionContext.ddID).get(conversionContext.aggregatorID);
    GPOUtils.copyPOJOToGPO(aggregates, aggregateGetters, input);

    return new InputEvent(new EventKey(conversionContext.schemaID,
                                       conversionContext.dimensionDescriptorID,
                                       conversionContext.aggregatorID,
                                       key),
                          aggregates);
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

  private void buildGetters(Object inputEvent)
  {
    if(builtGetters) {
      return;
    }

    builtGetters = true;

    Class<?> clazz = inputEvent.getClass();

    ddIDToKeyGetters = Lists.newArrayList();
    List<FieldsDescriptor> keyDescriptors = eventSchema.getDimensionsDescriptorIDToKeyDescriptor();
    ddIDToAggIDToGetters = Lists.newArrayList();

    for(int ddID = 0;
        ddID < eventSchema.getDimensionsDescriptorIDToKeyDescriptor().size();
        ddID++) {
      DimensionsDescriptor dd = eventSchema.getDimensionsDescriptorIDToDimensionsDescriptor().get(ddID);
      FieldsDescriptor keyDescriptor = keyDescriptors.get(ddID);
      GPOGetters keyGPOGetters = buildGPOGetters(clazz, keyDescriptor, dd, keyToExpression, true);
      ddIDToKeyGetters.add(keyGPOGetters);
      Int2ObjectMap<FieldsDescriptor> map = eventSchema.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor().get(ddID);
      IntArrayList aggIDList = eventSchema.getDimensionsDescriptorIDToAggregatorIDs().get(ddID);
      Int2ObjectOpenHashMap<GPOGetters> aggIDToGetters = new Int2ObjectOpenHashMap<GPOGetters>();
      ddIDToAggIDToGetters.add(aggIDToGetters);

      for(int aggIDIndex = 0;
          aggIDIndex < aggIDList.size();
          aggIDIndex++) {
        int aggID = aggIDList.get(aggIDIndex);
        FieldsDescriptor aggFieldsDescriptor = map.get(aggID);
        GPOGetters aggGPOGetters = buildGPOGetters(clazz, aggFieldsDescriptor, dd, aggregateToExpression, false);
        aggIDToGetters.put(aggID, aggGPOGetters);
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private GPOGetters buildGPOGetters(Class<?> clazz,
                                     FieldsDescriptor fieldsDescriptor,
                                     DimensionsDescriptor dd,
                                     Map<String, String> valueToExpression,
                                     boolean isKey)
  {
    GPOGetters gpoGetters = new GPOGetters();

    Map<Type, List<String>> typeToFields = fieldsDescriptor.getTypeToFields();

    for(Map.Entry<Type, List<String>> entry: typeToFields.entrySet()) {
      Type inputType = entry.getKey();
      List<String> fields = entry.getValue();

      switch(inputType) {
        case BOOLEAN: {
          gpoGetters.gettersBoolean = GPOUtils.createGetterBoolean(fields,
                                                                   valueToExpression,
                                                                   clazz);

          break;
        }
        case STRING: {
          gpoGetters.gettersString = GPOUtils.createGetterString(fields,
                                                                 valueToExpression,
                                                                 clazz);

          break;
        }
        case CHAR: {
          gpoGetters.gettersChar = GPOUtils.createGetterChar(fields,
                                                             valueToExpression,
                                                             clazz);

          break;
        }
        case DOUBLE: {
          gpoGetters.gettersDouble = GPOUtils.createGetterDouble(fields,
                                                                 valueToExpression,
                                                                 clazz);

          break;
        }
        case FLOAT: {
          gpoGetters.gettersFloat = GPOUtils.createGetterFloat(fields,
                                                               valueToExpression,
                                                               clazz);
          break;
        }
        case LONG: {
          gpoGetters.gettersLong = new GetterLong[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);
            GetterLong tempGetterLong = PojoUtils.createGetterLong(clazz, valueToExpression.get(field));

            if(isKey && field.equals(DimensionsDescriptor.DIMENSION_TIME)) {
              gpoGetters.gettersLong[getterIndex] = new GetTime(tempGetterLong, dd.getTimeBucket());
            }
            else {
              gpoGetters.gettersLong[getterIndex] = tempGetterLong;
            }
          }

          break;
        }
        case INTEGER: {
          gpoGetters.gettersInteger = new GetterInt[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);

            if(isKey && field.equals(DimensionsDescriptor.DIMENSION_TIME_BUCKET)) {
              gpoGetters.gettersInteger[getterIndex]
                      = new GetTimeBucket(dd.getTimeBucket());

            }
            else {
              gpoGetters.gettersInteger[getterIndex]
                      = PojoUtils.createGetterInt(clazz, valueToExpression.get(field));
            }
          }

          break;
        }
        case SHORT: {
          gpoGetters.gettersShort = GPOUtils.createGetterShort(fields,
                                                               valueToExpression,
                                                               clazz);

          break;
        }
        case BYTE: {
          gpoGetters.gettersByte = GPOUtils.createGetterByte(fields,
                                                             valueToExpression,
                                                             clazz);

          break;
        }
        case OBJECT: {
          gpoGetters.gettersObject = GPOUtils.createGetterObject(fields,
                                                                 valueToExpression,
                                                                 clazz);

          break;
        }
        default: {
          throw new IllegalArgumentException("The type " + inputType + " is not supported.");
        }
      }
    }

    return gpoGetters;
  }

  public static class GetTimeBucket implements PojoUtils.GetterInt<Object>
  {
    private final int timeBucket;

    public GetTimeBucket(TimeBucket timeBucket)
    {
      this.timeBucket = timeBucket.ordinal();
    }

    @Override
    public int get(Object obj)
    {
      return timeBucket;
    }
  }

  public static class GetTime implements PojoUtils.GetterLong<Object>
  {
    private final GetterLong<Object> timeGetter;
    private final TimeBucket timeBucket;

    public GetTime(GetterLong<Object> timeGetter, TimeBucket timeBucket)
    {
      this.timeGetter = Preconditions.checkNotNull(timeGetter);
      this.timeBucket = Preconditions.checkNotNull(timeBucket);
    }

    @Override
    public long get(Object obj)
    {
      long time = timeGetter.get(obj);
      return timeBucket.roundDown(time);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsComputationFlexibleSingleSchemaPOJO.class);
}
