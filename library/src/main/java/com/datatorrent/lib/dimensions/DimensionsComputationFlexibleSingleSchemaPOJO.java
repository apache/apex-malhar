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
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterByte;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterShort;
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
    copyPOJOToGPO(key, keyGetters, input);

    GPOMutable aggregates = new GPOMutable(conversionContext.aggregateDescriptor);
    GPOGetters aggregateGetters =
    ddIDToAggIDToGetters.get(conversionContext.ddID).get(conversionContext.aggregatorID);
    copyPOJOToGPO(aggregates, aggregateGetters, input);

    return new InputEvent(new EventKey(conversionContext.schemaID,
                                       conversionContext.dimensionDescriptorID,
                                       conversionContext.aggregatorID,
                                       key),
                          aggregates);
  }

  private void copyPOJOToGPO(GPOMutable mutable, GPOGetters getters, Object object)
  {
    {
      boolean[] tempBools = mutable.getFieldsBoolean();
      GetterBoolean<Object>[] tempGetterBools = getters.gettersBoolean;

      if(tempBools != null) {
        for(int index = 0;
            index < tempBools.length;
            index++) {
          tempBools[index] = tempGetterBools[index].get(object);
        }
      }
    }

    {
      byte[] tempBytes = mutable.getFieldsByte();
      GetterByte<Object>[] tempGetterByte = getters.gettersByte;

      if(tempBytes != null) {
        for(int index = 0;
            index < tempBytes.length;
            index++) {
          tempBytes[index] = tempGetterByte[index].get(object);
        }
      }
    }

    {
      char[] tempChar = mutable.getFieldsCharacter();
      GetterChar<Object>[] tempGetterChar = getters.gettersChar;

      if(tempChar != null) {
        for(int index = 0;
            index < tempChar.length;
            index++) {
          tempChar[index] = tempGetterChar[index].get(object);
        }
      }
    }

    {
      double[] tempDouble = mutable.getFieldsDouble();
      GetterDouble<Object>[] tempGetterDouble = getters.gettersDouble;

      if(tempDouble != null) {
        for(int index = 0;
            index < tempDouble.length;
            index++) {
          tempDouble[index] = tempGetterDouble[index].get(object);
        }
      }
    }

    {
      float[] tempFloat = mutable.getFieldsFloat();
      GetterFloat<Object>[] tempGetterFloat = getters.gettersFloat;

      if(tempFloat != null) {
        for(int index = 0;
            index < tempFloat.length;
            index++) {
          tempFloat[index] = tempGetterFloat[index].get(object);
        }
      }
    }

    {
      int[] tempInt = mutable.getFieldsInteger();
      GetterInt<Object>[] tempGetterInt = getters.gettersInteger;

      if(tempInt != null) {
        for(int index = 0;
            index < tempInt.length;
            index++) {
          tempInt[index] = tempGetterInt[index].get(object);
        }
      }
    }

    {
      long[] tempLong = mutable.getFieldsLong();
      GetterLong<Object>[] tempGetterLong = getters.gettersLong;

      if(tempLong != null) {
        for(int index = 0;
            index < tempLong.length;
            index++) {
          tempLong[index] = tempGetterLong[index].get(object);
        }
      }
    }

    {
      short[] tempShort = mutable.getFieldsShort();
      GetterShort<Object>[] tempGetterShort = getters.gettersShort;

      if(tempShort != null) {
        for(int index = 0;
            index < tempShort.length;
            index++) {
          tempShort[index] = tempGetterShort[index].get(object);
        }
      }
    }

    {
      String[] tempString = mutable.getFieldsString();
      Getter<Object, String>[] tempGetterString = getters.gettersString;

      if(tempString != null) {
        for(int index = 0;
            index < tempString.length;
            index++) {
          tempString[index] = tempGetterString[index].get(object);
        }
      }
    }
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
    List<FieldsDescriptor> keyDescriptors = eventSchema.getDdIDToKeyDescriptor();
    ddIDToAggIDToGetters = Lists.newArrayList();

    for(int ddID = 0;
        ddID < eventSchema.getDdIDToKeyDescriptor().size();
        ddID++) {
      DimensionsDescriptor dd = eventSchema.getDdIDToDD().get(ddID);
      FieldsDescriptor keyDescriptor = keyDescriptors.get(ddID);
      GPOGetters keyGPOGetters = buildGPOGetters(clazz, keyDescriptor, dd, keyToExpression, true);
      ddIDToKeyGetters.add(keyGPOGetters);
      Int2ObjectMap<FieldsDescriptor> map = eventSchema.getDdIDToAggIDToInputAggDescriptor().get(ddID);
      IntArrayList aggIDList = eventSchema.getDdIDToAggIDs().get(ddID);
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
          gpoGetters.gettersBoolean = new GetterBoolean[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);
            gpoGetters.gettersBoolean[getterIndex]
                    = PojoUtils.createGetterBoolean(clazz, valueToExpression.get(field));
          }

          break;
        }
        case STRING: {
          gpoGetters.gettersString = new Getter[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);
            gpoGetters.gettersString[getterIndex]
                    = PojoUtils.createGetter(clazz, valueToExpression.get(field), String.class);
          }

          break;
        }
        case CHAR: {
          gpoGetters.gettersChar = new GetterChar[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);
            gpoGetters.gettersChar[getterIndex]
                    = PojoUtils.createGetterChar(clazz, valueToExpression.get(field));
          }

          break;
        }
        case DOUBLE: {
          gpoGetters.gettersDouble = new GetterDouble[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);
            gpoGetters.gettersDouble[getterIndex]
                    = PojoUtils.createGetterDouble(clazz, valueToExpression.get(field));
          }

          break;
        }
        case FLOAT: {
          gpoGetters.gettersFloat = new GetterFloat[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);
            gpoGetters.gettersFloat[getterIndex]
                    = PojoUtils.createGetterFloat(clazz, valueToExpression.get(field));
          }

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
          gpoGetters.gettersShort = new GetterShort[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);
            gpoGetters.gettersShort[getterIndex]
                    = PojoUtils.createGetterShort(clazz, valueToExpression.get(field));
          }

          break;
        }
        case BYTE: {
          gpoGetters.gettersByte = new GetterByte[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);
            gpoGetters.gettersByte[getterIndex]
                    = PojoUtils.createGetterByte(clazz, valueToExpression.get(field));
          }

          break;
        }
        case OBJECT: {
          gpoGetters.gettersObject = new Getter[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);
            gpoGetters.gettersObject[getterIndex]
                    = PojoUtils.createGetter(clazz, valueToExpression.get(field), Object.class);
          }

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

  public static class GPOGetters
  {
    public GetterBoolean<Object>[] gettersBoolean;
    public GetterChar<Object>[] gettersChar;
    public GetterByte<Object>[] gettersByte;
    public GetterShort<Object>[] gettersShort;
    public GetterInt<Object>[] gettersInteger;
    public GetterLong<Object>[] gettersLong;
    public GetterFloat<Object>[] gettersFloat;
    public GetterDouble<Object>[] gettersDouble;
    public Getter<Object, String>[] gettersString;
    public Getter<Object, Object>[] gettersObject;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsComputationFlexibleSingleSchemaPOJO.class);
}
