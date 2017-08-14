/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.dimensions.aggregator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.apex.malhar.lib.appdata.gpo.GPOMutable;
import org.apache.apex.malhar.lib.appdata.schemas.Type;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.EventKey;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/**
 * @since 3.4.0
 */
public abstract class AbstractTopBottomAggregator extends AbstractCompositeAggregator
{
  public static final String PROP_COUNT = "count";
  protected int count;
  protected SortedSet<String> subCombinations = Sets.newTreeSet();

  public AbstractTopBottomAggregator withEmbedAggregatorName(String embedAggregatorName)
  {
    this.setEmbedAggregatorName(embedAggregatorName);
    return this;
  }

  public AbstractTopBottomAggregator withSubCombinations(String[] subCombinations)
  {
    this.setSubCombinations(subCombinations);
    return this;
  }

  public AbstractTopBottomAggregator withCount(int count)
  {
    this.setCount(count);
    return this;
  }

  public int getCount()
  {
    return count;
  }

  public void setCount(int count)
  {
    this.count = count;
  }

  public void setSubCombinations(Set<String> subCombinations)
  {
    this.subCombinations.clear();
    this.subCombinations.addAll(subCombinations);
  }

  public void setSubCombinations(String[] subCombinations)
  {
    setSubCombinations(Sets.newHashSet(subCombinations));
  }

  public Set<String> getSubCombinations()
  {
    return subCombinations;
  }

  /**
   * TOP/BOTTOM return a list of value
   */
  @Override
  public Type getOutputType()
  {
    return Type.OBJECT;
  }

  @Override
  public int hashCode()
  {
    return (embedAggregatorName.hashCode() * 31 + count) * 31 + subCombinations.hashCode();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    AbstractTopBottomAggregator other = (AbstractTopBottomAggregator)obj;
    if (embedAggregatorName != other.embedAggregatorName
        && (embedAggregatorName == null || !embedAggregatorName.equals(other.embedAggregatorName))) {
      return false;
    }
    if (count != other.count) {
      return false;
    }
    if (subCombinations != other.subCombinations
        && (subCombinations == null || !subCombinations.equals(other.subCombinations))) {
      return false;
    }

    return true;
  }


  /**
   * The result keep a list of object for each aggregate value
   * The value of resultAggregate should keep a list of inputEventKey(the value can be get from cache or load) or a map
   * from inputEventKey to the value instead of just a list of aggregate value. As the value could be changed in
   * current window, and this change should be applied.
   *
   * precondition: resultAggregate.eventKey matches with inputSubEventKeys
   * notes: this algorithm only support TOP for positive values and BOTTOM for negative values
   */
  @Override
  public void aggregate(Aggregate resultAggregate, Set<EventKey> inputSubEventKeys,
      Map<EventKey, Aggregate> inputAggregatesRepo)
  {
    //there are problem for composite's value field descriptor, just ignore now.
    GPOMutable resultGpo = resultAggregate.getAggregates();
    final List<String> compositeFieldList = resultAggregate.getEventKey().getKey().getFieldDescriptor().getFieldList();

    //Map<EventKey, Aggregate> existedSubEventKeyToAggregate = Maps.newHashMap();
    for (String valueField : resultGpo.getFieldDescriptor().getFieldList()) {
      //the resultGpo keep a list of sub aggregates
      updateAggregate(resultAggregate, valueField, inputSubEventKeys, inputAggregatesRepo);

      //compare the existed sub aggregates with the new input aggregates to update the list
      for (EventKey eventKey : inputSubEventKeys) {
        aggregate(compositeFieldList, resultGpo, eventKey, inputAggregatesRepo.get(eventKey).getAggregates());
      }
    }

  }

  protected transient List<String> tmpStoreFieldList = Lists.newArrayList();
  protected static final String KEY_VALUE_SEPERATOR = "-";

  /**
   * get store map key from the eventKey
   *
   * @param eventKey
   * @return
   */
  protected String getStoreMapKey(EventKey subEventKey, List<String> compositeEventFieldList)
  {
    tmpStoreFieldList.clear();
    tmpStoreFieldList.addAll(subEventKey.getKey().getFieldDescriptor().getFieldList());
    tmpStoreFieldList.removeAll(compositeEventFieldList);
    Collections.sort(tmpStoreFieldList);
    StringBuilder key = new StringBuilder();
    for (String field: tmpStoreFieldList) {
      key.append(subEventKey.getKey().getField(field)).append(KEY_VALUE_SEPERATOR);
    }
    key.deleteCharAt(key.length() - 1);

    return key.toString();
  }


  /**
   * update existed sub aggregate.
   * The sub aggregates which kept in composite aggregate as candidate could be changed. synchronize the value with
   * input aggregates.
   *
   * @param resultAggregate
   * @param valueField
   * @param inputSubEventKeys
   * @param inputAggregatesRepo
   */
  @SuppressWarnings("unchecked")
  protected void updateAggregate(Aggregate resultAggregate, String valueField,
      Set<EventKey> inputSubEventKeys, Map<EventKey, Aggregate> inputAggregatesRepo)
  {
    Map<String, Object> resultAggregateFieldToValue =
        (Map<String, Object>)resultAggregate.getAggregates().getFieldObject(valueField);
    if (resultAggregateFieldToValue == null) {
      return;
    }

    for (EventKey inputSubEventKey : inputSubEventKeys) {
      Aggregate inputSubAggregate = inputAggregatesRepo.get(inputSubEventKey);
      String mapKey = getStoreMapKey(inputSubAggregate.getEventKey(),
          resultAggregate.getEventKey().getKey().getFieldDescriptor().getFieldList());
      //Aggregate existedAggregate = existedSubEventKeyToAggregate.get(inputSubEventKey);
      if (resultAggregateFieldToValue.get(mapKey) != null) {
        resultAggregateFieldToValue.put(mapKey, inputSubAggregate.getAggregates().getField(valueField));
      }
    }
  }

  /**
   * need a map of value field from the inputGpo to resultGpo, use the index of Fields as the index
   * @param resultGpo
   * @param inputGpo
   */
  @SuppressWarnings("unchecked")
  protected void aggregate(final List<String> compositeFieldList, GPOMutable resultGpo,
      EventKey subEventKey, GPOMutable inputGpo)
  {
    //the field and type should get from resultGpo instead of inputGpo as inputGpo maybe shared by other value fields
    List<String> aggregateFields = resultGpo.getFieldDescriptor().getFieldList();
    Map<String, Type> fieldToType = resultGpo.getFieldDescriptor().getFieldToType();
    for (String aggregateField : aggregateFields) {
      Map<String, Object> fieldValue = (Map<String, Object>)resultGpo.getFieldObject(aggregateField);
      if (fieldValue == null) {
        fieldValue = createAggregateValueForField(aggregateField, fieldToType.get(aggregateField));
        resultGpo.setFieldObject(aggregateField, fieldValue);
      }
      aggregate(compositeFieldList, fieldValue, subEventKey, inputGpo.getField(aggregateField),
          fieldToType.get(aggregateField));
    }
  }

  /**
   * seperate it in case sub class override it.
   * @param fieldName
   * @param fieldElementType
   * @return
   */
  protected Map<String, Object> createAggregateValueForField(String fieldName, Type fieldElementType)
  {
    return Maps.newHashMap();
  }

  /**
   * compare the result(resultMap) with input(inputFieldName, inputFieldValue)
   * @param resultMap
   * @param inputFieldValue
   * @param type
   */
  protected void aggregate(final List<String> compositeFieldList, Map<String, Object> resultMap,
      EventKey subEventKey, Object inputFieldValue, Type type)
  {
    if (resultMap.size() < count) {
      resultMap.put(getStoreMapKey(subEventKey, compositeFieldList), inputFieldValue);
      return;
    }
    for (String key : resultMap.keySet()) {
      Object resultValue = resultMap.get(key);
      if (shouldReplaceResultElement(resultValue, inputFieldValue, type)) {
        resultMap.put(key, inputFieldValue);
        break;
      }
    }

  }

  /**
   * shoud the result element replaced by input element.
   * the inputElement and resultElement should be same type
   * @param resultElement
   * @param inputElement
   * @param type
   * @return
   */
  protected boolean shouldReplaceResultElement(Object resultElement, Object inputElement, Type type)
  {
    if (inputElement == null) {
      return false;
    }

    if (resultElement == null) {
      return true;
    }

    if (resultElement instanceof Comparable) {
      @SuppressWarnings("unchecked")
      int compareResult = ((Comparable<Object>)resultElement).compareTo(inputElement);
      return shouldReplaceResultElement(compareResult);
    }

    //handle other cases
    throw new RuntimeException("Should NOT come here.");

  }

  protected abstract boolean shouldReplaceResultElement(int resultCompareToInput);
}
