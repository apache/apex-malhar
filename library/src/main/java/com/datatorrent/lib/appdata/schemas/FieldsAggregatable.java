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

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FieldsAggregatable
{
  private Map<String, Set<String>> fieldToAggregator;
  private Map<String, Set<String>> aggregatorToField;
  private Map<String, Map<String, String>> aggregatorToFieldToName;
  private Fields aggregatedFields;
  private Fields nonAggregatedFields;
  private Set<String> fieldNames;
  private FieldsDescriptor fd;
  private Set<String> aggregators;

  public FieldsAggregatable(Set<String> fields,
                            Map<String, Set<String>> fieldToAggregator)
  {
    setNonAggregatedFields(fields);
    setFieldToAggregator(fieldToAggregator);

    initialize();
  }

  public FieldsAggregatable(Map<String, Set<String>> fieldToAggregator)
  {
    setFieldToAggregator(fieldToAggregator);
    nonAggregatedFields = new Fields(new HashSet<String>());

    initialize();
  }

  private void setNonAggregatedFields(Set<String> nonAggregatedFields)
  {
    this.nonAggregatedFields = new Fields(nonAggregatedFields);
  }

  private void setFieldToAggregator(Map<String, Set<String>> fieldToAggregator)
  {
    this.fieldToAggregator = Maps.newHashMap();

    for(Map.Entry<String, Set<String>> entry: fieldToAggregator.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());

      for(String aggregator: entry.getValue()) {
        Preconditions.checkNotNull(aggregator);
      }

      this.fieldToAggregator.put(entry.getKey(),
                                 Sets.newHashSet(entry.getValue()));
    }
  }

  private void initialize()
  {
    aggregatedFields = new Fields(fieldToAggregator.keySet());
  }

  public Set<String> getAggregators(String field)
  {
    return fieldToAggregator.get(field);
  }

  public Fields getAggregatedFields()
  {
    return aggregatedFields;
  }

  public Fields getNonAggregatedFields()
  {
    return nonAggregatedFields;
  }

  public Set<String> getAggregators()
  {
    if(aggregators != null) {
      return aggregators;
    }

    aggregators = Sets.newHashSet();

    for(Map.Entry<String, Set<String>> entry: fieldToAggregator.entrySet()) {
      aggregators.addAll(entry.getValue());
    }

    return aggregators;
  }

  public Set<String> getFieldNames()
  {
    StringBuilder sb = new StringBuilder();

    if(fieldNames != null) {
      return fieldNames;
    }

    fieldNames = Sets.newHashSet();

    for(Map.Entry<String, Set<String>> entry: fieldToAggregator.entrySet()) {
      String value = entry.getKey();
      for(String aggregator: entry.getValue()) {
        sb.append(value);
        sb.append(DimensionalEventSchema.ADDITIONAL_VALUE_SEPERATOR);
        sb.append(aggregator);
        fieldNames.add(sb.toString());

        sb.delete(0, sb.length());
      }
    }

    for(String field: nonAggregatedFields.getFields()) {
      fieldNames.add(field);
    }

    return fieldNames;
  }

  public FieldsDescriptor buildFieldsDescriptor(FieldsDescriptor inputValuesDescriptor,
                                                Map<String, DimensionsAggregator> nameToAggregator)
  {
    StringBuilder sb = new StringBuilder();
    Map<String, Type> fieldToType = Maps.newHashMap();

    for(Map.Entry<String, Set<String>> entry: fieldToAggregator.entrySet()) {
      String value = entry.getKey();
      Type inputType = inputValuesDescriptor.getType(value);

      for(String aggregatorName: entry.getValue()) {
        sb.append(value);
        sb.append(DimensionalEventSchema.ADDITIONAL_VALUE_SEPERATOR);
        sb.append(aggregatorName);

        String fieldValue = sb.toString();
        sb.delete(0, sb.length());
        Type aggType = nameToAggregator.get(aggregatorName).getTypeConversionMap().get(inputType);
        fieldToType.put(fieldValue, aggType);
      }
    }

    for(String nonAggregatedField: nonAggregatedFields.getFields()) {
      Type nonAggregatedFieldType =
      DimensionsDescriptor.DIMENSION_FIELD_TO_TYPE.get(nonAggregatedField);

      fieldToType.put(nonAggregatedField, nonAggregatedFieldType);
    }

    fd = new FieldsDescriptor(fieldToType);
    return fd;
  }

  public FieldsDescriptor getFieldsDescriptor()
  {
    if(fd == null) {
      throw new UnsupportedOperationException("A field descriptor must first be built with buildFieldsDescriptor");
    }

    return fd;
  }

  public Map<String, Set<String>> getAggregatorToFields()
  {
    if(aggregatorToField != null) {
      return aggregatorToField;
    }

    Map<String, Set<String>> aggregatorToFields = Maps.newHashMap();

    for(Map.Entry<String, Set<String>> entry: fieldToAggregator.entrySet()) {
      String field = entry.getKey();
      Set<String> aggregators = entry.getValue();

      for(String aggregatorName: aggregators) {
        Set<String> fieldSet = aggregatorToFields.get(aggregatorName);

        if(fieldSet == null) {
          fieldSet = Sets.newHashSet();
          aggregatorToFields.put(aggregatorName, fieldSet);
        }

        fieldSet.add(field);
      }
    }

    aggregatorToField = Maps.newHashMap();

    for(Map.Entry<String, Set<String>> entry: aggregatorToFields.entrySet()) {
      aggregatorToField.put(entry.getKey(), Collections.unmodifiableSet(entry.getValue()));
    }

    aggregatorToField = Collections.unmodifiableMap(aggregatorToField);
    return aggregatorToField;
  }

  public Map<String, Map<String, String>> getAggregatorToFieldToName()
  {
    if(aggregatorToFieldToName != null) {
      return aggregatorToFieldToName;
    }

    Map<String, Set<String>> aggregatorToFields = getAggregatorToFields();
    Map<String, Map<String, String>> tAggregatorToFieldToName = Maps.newHashMap();

    for(Map.Entry<String, Set<String>> entry: aggregatorToFields.entrySet()) {
      String aggregatorName = entry.getKey();
      Set<String> fields = entry.getValue();

      Map<String, String> fieldToName = Maps.newHashMap();
      tAggregatorToFieldToName.put(aggregatorName, fieldToName);

      for(String field: fields) {
        fieldToName.put(field, field +
                               DimensionalEventSchema.ADDITIONAL_VALUE_SEPERATOR +
                               aggregatorName);
      }
    }

    aggregatorToFieldToName = Maps.newHashMap();

    for(Map.Entry<String, Map<String, String>> entry: tAggregatorToFieldToName.entrySet()) {
      aggregatorToFieldToName.put(entry.getKey(), Collections.unmodifiableMap(entry.getValue()));
    }

    aggregatorToFieldToName = Collections.unmodifiableMap(aggregatorToFieldToName);
    return aggregatorToFieldToName;
  }
  
  @Override
  public String toString()
  {
    return "FieldsAggregatable{" + "fieldToAggregator=" + fieldToAggregator + ", nonAggregatedFields=" + nonAggregatedFields + '}';
  }
}
