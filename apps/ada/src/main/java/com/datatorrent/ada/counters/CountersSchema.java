/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

import com.datatorrent.ada.dimensions.AggregatorType;
import com.datatorrent.lib.appdata.dimensions.DimensionsSchema;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.qr.DataDeserializerInfo;
import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.DataValidatorInfo;
import com.datatorrent.lib.appdata.qr.SimpleDataDeserializer;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DataType(type=CountersSchema.TYPE)
@DataDeserializerInfo(clazz=SimpleDataDeserializer.class)
@DataValidatorInfo(clazz=CountersSchemaValidator.class)
public class CountersSchema extends CountersData implements DimensionsSchema
{
  public static final String TYPE = "schema";

  public static final String FIELD_KEYS = "keys";
  public static final String FIELD_VALUES = "values";
  public static final String FIELD_TIME_BUCKETS = "timeBuckets";

  public static final String FIELD_VALUE_NAME = "name";
  public static final String FIELD_VALUE_AGGREGATIONS = "aggregations";
  public static final String FIELD_VALUE_TYPE = "type";

  public static final String AGGREGATION_COUNT = "count";
  public static final String AGGREGATION_SUM = "sum";
  public static final String AGGREGATION_MIN = "min";
  public static final String AGGREGATION_MAX = "max";
  public static final Set<String> AGGREGATIONS = ImmutableSet.of(AGGREGATION_COUNT,
                                                                 AGGREGATION_SUM,
                                                                 AGGREGATION_MIN,
                                                                 AGGREGATION_MAX);

  public static final String TIME_BUCKET_MINUTE = "1m";
  public static final String TIME_BUCKET_HOUR = "1h";
  public static final String TIME_BUCKET_DAY = "1d";
  public static final Set<String> TIME_BUCKETS = ImmutableSet.of(TIME_BUCKET_MINUTE,
                                                                 TIME_BUCKET_HOUR,
                                                                 TIME_BUCKET_DAY);

  private Map<String, String> keys;
  @NotNull
  @Valid
  private List<CountersSchemaValues> values;
  private List<String> timeBuckets;

  @JsonIgnore
  private FieldsDescriptor keyFieldDescriptor;
  @JsonIgnore
  private FieldsDescriptor aggregateFieldDescriptor;
  @JsonIgnore
  private FieldsDescriptor allFieldDescriptor;
  @JsonIgnore
  private Map<String, CountersSchemaValues> namesToValues;
  @JsonIgnore
  private Map<String, Set<String>> aggregateToFields;
  @JsonIgnore
  private Map<String, FieldsDescriptor> aggregateToFieldsDescriptor;
  @JsonIgnore
  private Map<String, List<String>> fieldToAggregators;

  public CountersSchema()
  {
  }

  @JsonIgnore
  public DataGroup getDataGroup()
  {
    return new DataGroup(this.getUser(),
                         this.getAppName(),
                         this.getLogicalOperatorName(),
                         this.getVersion());
  }


  @JsonIgnore
  public Map<String, Set<String>> getAggregateToFields()
  {
    if(aggregateToFields != null) {
      return aggregateToFields;
    }

    getFieldToAggregators();
    aggregateToFields = Maps.newHashMap();

    for(Map.Entry<String, List<String>> entry: fieldToAggregators.entrySet()) {
      String field = entry.getKey();

      for(String aggregator: entry.getValue()) {
        Set<String> fields = aggregateToFields.get(aggregator);

        if(fields == null) {
          fields = Sets.newHashSet();
          aggregateToFields.put(aggregator, fields);
        }

        fields.add(field);
      }
    }

    for(Map.Entry<String, Set<String>> entry: aggregateToFields.entrySet()) {
      aggregateToFields.put(entry.getKey(),
                            Collections.unmodifiableSet(aggregateToFields.get(entry.getKey())));
    }

    aggregateToFields = Collections.unmodifiableMap(aggregateToFields);

    return aggregateToFields;
  }

  @JsonIgnore
  public Map<String, FieldsDescriptor> getAggregateToFieldDescriptor()
  {
    if(aggregateToFieldsDescriptor != null) {
      return aggregateToFieldsDescriptor;
    }

    getAggregateToFields();
    getAllFieldsDescriptor();
    aggregateToFieldsDescriptor = Maps.newHashMap();

    for(Map.Entry<String, Set<String>> entry: aggregateToFields.entrySet()) {
      String aggregator = entry.getKey();
      Set<String> fields = entry.getValue();

      Type aggType = AggregatorType.NAME_TO_AGGREGATOR_TYPE.get(aggregator).getType();
      Map<String, Type> fieldsToType = Maps.newHashMap();

      if(aggType != null) {
        for(String field: fields) {
          fieldsToType.put(field, aggType);
        }
      }
      else {
        for(String field: fields) {
          Type type = allFieldDescriptor.getType(field);
          fieldsToType.put(field, type);
        }
      }

      FieldsDescriptor fd = new FieldsDescriptor(fieldsToType);
      aggregateToFieldsDescriptor.put(aggregator, fd);
    }

    aggregateToFieldsDescriptor = Collections.unmodifiableMap(aggregateToFieldsDescriptor);
    return aggregateToFieldsDescriptor;
  }

  @JsonIgnore
  public Map<String, List<String>> getFieldToAggregators()
  {
    if(fieldToAggregators != null) {
      return fieldToAggregators;
    }

    getNamesToValues();
    fieldToAggregators = Maps.newHashMap();

    for(Map.Entry<String, CountersSchemaValues> entry: namesToValues.entrySet()) {
      String field = entry.getKey();
      CountersSchemaValues csv = entry.getValue();

      List<String> aggregations = fieldToAggregators.get(field);

      if(aggregations == null) {
        aggregations = Lists.newArrayList();
        fieldToAggregators.put(field, aggregations);
      }

      aggregations.addAll(csv.getAggregations());
    }

    for(Map.Entry<String, List<String>> entry: fieldToAggregators.entrySet()) {
      fieldToAggregators.put(entry.getKey(),
                             Collections.unmodifiableList(entry.getValue()));
    }

    fieldToAggregators = Collections.unmodifiableMap(fieldToAggregators);

    return fieldToAggregators;
  }

  @JsonIgnore
  public Map<String, CountersSchemaValues> getNamesToValues()
  {
    if(namesToValues != null) {
      return namesToValues;
    }

    namesToValues = Maps.newHashMap();

    for(CountersSchemaValues csvs: values) {
      namesToValues.put(csvs.getName(), csvs);
    }

    namesToValues = Collections.unmodifiableMap(namesToValues);

    return namesToValues;
  }

  /**
   * @return the keys
   */
  public Map<String, String> getKeys()
  {
    if(keys == null) {
      keys = Maps.newHashMap();
    }

    return keys;
  }

  /**
   * @param keys the keys to set
   */
  public void setKeys(Map<String, String> keys)
  {
    this.keys = keys;
  }

  /**
   * @return the values
   */
  public List<CountersSchemaValues> getValues()
  {
    return values;
  }

  /**
   * @param values the values to set
   */
  public void setValues(List<CountersSchemaValues> values)
  {
    this.values = values;
  }

  @JsonIgnore
  @Override
  public FieldsDescriptor getKeyFieldDescriptor()
  {
    if(keyFieldDescriptor != null) {
      return keyFieldDescriptor;
    }

    keyFieldDescriptor =  new FieldsDescriptor(GPOUtils.buildTypeMap(getKeys()));
    return keyFieldDescriptor;
  }

  @JsonIgnore
  @Override
  public FieldsDescriptor getAggregateFieldDescriptor()
  {
    if(aggregateFieldDescriptor != null) {
      return aggregateFieldDescriptor;
    }

    Map<String, String> aggregatesToTypes = Maps.newHashMap();

    for(CountersSchemaValues value: getValues()) {
      aggregatesToTypes.put(value.getName(), value.getType());
    }

    aggregateFieldDescriptor = new FieldsDescriptor(GPOUtils.buildTypeMap(aggregatesToTypes));
    return aggregateFieldDescriptor;
  }

  @JsonIgnore
  @Override
  public FieldsDescriptor getAllFieldsDescriptor()
  {
    if(allFieldDescriptor != null) {
      return allFieldDescriptor;
    }

    getKeyFieldDescriptor();
    getAggregateFieldDescriptor();

    Map<String, Type> fieldsToType = Maps.newHashMap();

    for(String field: keyFieldDescriptor.getFields().getFields()) {
      fieldsToType.put(field, keyFieldDescriptor.getType(field));
    }

    for(String field: aggregateFieldDescriptor.getFields().getFields()) {
      fieldsToType.put(field, aggregateFieldDescriptor.getType(field));
    }

    allFieldDescriptor = new FieldsDescriptor(fieldsToType);
    return allFieldDescriptor;
  }

  /**
   * @return the timeBuckets
   */
  public List<String> getTimeBuckets()
  {
    if(timeBuckets == null) {
      timeBuckets = Lists.newArrayList();
    }

    return timeBuckets;
  }

  /**
   * @param timeBuckets the timeBuckets to set
   */
  public void setTimeBuckets(List<String> timeBuckets)
  {
    this.timeBuckets = timeBuckets;
  }

  public static class CountersSchemaValues
  {
    @NotNull
    private String name;
    @NotNull
    private String type;
    private List<String> aggregations;

    public CountersSchemaValues()
    {
    }

    /**
     * @return the name
     */
    public String getName()
    {
      return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name)
    {
      this.name = name;
    }

    /**
     * @return the type
     */
    public String getType()
    {
      return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type)
    {
      this.type = type;
    }

    /**
     * @return the aggregations
     */
    public List<String> getAggregations()
    {
      if(aggregations == null) {
        aggregations = Lists.newArrayList();
      }

      return aggregations;
    }

    /**
     * @param aggregations the aggregations to set
     */
    public void setAggregations(List<String> aggregations)
    {
      this.aggregations = aggregations;
    }
  }
}
