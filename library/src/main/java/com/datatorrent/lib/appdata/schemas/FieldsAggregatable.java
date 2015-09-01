/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.schemas;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This is a helper class used internally for processing queries issued against the {@link DimensionalSchema}.
 * It maintains information about which fields do not have any aggregations (time would be an example of such a field).
 * And it also maintains what aggregations aggregatable fields have applied to them.
 * @since 3.1.0
 *
 */
public class FieldsAggregatable
{
  /**
   * This is a map from a field to the set of all aggregation that are performed on that field.
   */
  private Map<String, Set<String>> fieldToAggregator;
  /**
   * This is a map from an aggregator to the set of all fields which are aggregated using that aggregator.
   */
  private Map<String, Set<String>> aggregatorToField;
  /**
   * This is a map from aggregator to field name to the field names used in {@link DataQueryDimensional} queries.
   * The field names used in {@link DataQueryDimensional} queries are the name of the field appended with a colon and
   * the name of the aggregator (FIELD:AGGREGATOR).
   */
  private Map<String, Map<String, String>> aggregatorToFieldToName;
  /**
   * The set of fields which has aggregations applied to them.
   */
  private Fields aggregatedFields;
  /**
   * The set of fields which do not have aggregations applied to them.
   */
  private Fields nonAggregatedFields;
  /**
   * The set of all field names managed by this {@link FieldsAggregatable} object.
   */
  private Set<String> fieldNames;
  /**
   * The set of all aggregators applied on fields managed by this {@link FieldsAggregatable} object.
   */
  private Set<String> aggregators;

  /**
   * The is creates a {@link FieldsAggregatable} object.
   * @param fields The non aggregatable fields managed by this {@link FieldsAggregatable} object.
   * @param fieldToAggregator The aggregatable fields managed by this object and a set of all the
   * aggregators applied to each aggregatable fields.
   */
  public FieldsAggregatable(Set<String> fields,
                            Map<String, Set<String>> fieldToAggregator)
  {
    setNonAggregatedFields(fields);
    setFieldToAggregator(fieldToAggregator);

    initialize();
  }

  /**
   *
   * @param fieldToAggregator
   */
  public FieldsAggregatable(Map<String, Set<String>> fieldToAggregator)
  {
    setFieldToAggregator(fieldToAggregator);
    nonAggregatedFields = new Fields(new HashSet<String>());

    initialize();
  }

  /**
   * This is a helper method for setting the non aggregatable fields.
   * @param nonAggregatedFields The non aggregatable fields managed by this
   * {@link FieldsAggregatable} object.
   */
  private void setNonAggregatedFields(Set<String> nonAggregatedFields)
  {
    this.nonAggregatedFields = new Fields(nonAggregatedFields);
  }

  /**
   * This is a helper method for setting the map from the field to the set of all
   * the aggregators managed by this {@link FieldsAggregatable} object.
   * @param fieldToAggregator A map from the field to the set of all aggregators applied to that field.
   */
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

  /**
   * Helper method to initialize internal datastructures.
   */
  private void initialize()
  {
    aggregatedFields = new Fields(fieldToAggregator.keySet());
  }

  /**
   * Convenience method to get the set of all aggregators applied on a field.
   * @param field The field to get the set of aggregators for.
   * @return The set of all aggregators applied to the given field.
   */
  public Set<String> getAggregators(String field)
  {
    return fieldToAggregator.get(field);
  }

  /**
   * Returns the set of all aggregated fields.
   * @return The set of all aggregated fields.
   */
  public Fields getAggregatedFields()
  {
    return aggregatedFields;
  }

  /**
   * The set of all non aggregated fields.
   * @return The set of all non aggregated fields.
   */
  public Fields getNonAggregatedFields()
  {
    return nonAggregatedFields;
  }

  /**
   * Gets the set of all aggregators applied to a field managed by this {@link FieldsAggregatable} object.
   * @return The set of all aggregators applied to a field.
   */
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

  /**
   * Gets the set of all fields managed by this {@link FieldsAggregatable} object.
   * @return The set of all fields managed by this {@link FieldsAggregatable} object.
   */
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
        sb.append(DimensionalConfigurationSchema.ADDITIONAL_VALUE_SEPERATOR);
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

  /**
   * Returns a map from aggregator names to a set of all the fields that
   * that aggregator is applied to.
   * @return A map from aggregator names to a set of all the fields that
   * that aggregator is applied to.
   */
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

  /**
   * Returns a map from aggregator to field name to the field name with the aggregator name appended to it.
   * @return a map from aggregator to field name to the field name with the aggregator name appended to it.
   */
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
                               DimensionalConfigurationSchema.ADDITIONAL_VALUE_SEPERATOR +
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
