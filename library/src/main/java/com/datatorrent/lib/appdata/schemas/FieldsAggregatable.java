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
  public int hashCode()
  {
    int hash = 7;
    hash = 97 * hash + (this.fieldToAggregator != null ? this.fieldToAggregator.hashCode() : 0);
    hash = 97 * hash + (this.nonAggregatedFields != null ? this.nonAggregatedFields.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if(obj == null) {
      return false;
    }
    if(getClass() != obj.getClass()) {
      return false;
    }
    final FieldsAggregatable other = (FieldsAggregatable)obj;
    if(this.fieldToAggregator != other.fieldToAggregator && (this.fieldToAggregator == null || !this.fieldToAggregator.equals(other.fieldToAggregator))) {
      return false;
    }
    if(this.nonAggregatedFields != other.nonAggregatedFields && (this.nonAggregatedFields == null || !this.nonAggregatedFields.equals(other.nonAggregatedFields))) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "FieldsAggregatable{" + "fieldToAggregator=" + fieldToAggregator + ", nonAggregatedFields=" + nonAggregatedFields + '}';
  }
}
