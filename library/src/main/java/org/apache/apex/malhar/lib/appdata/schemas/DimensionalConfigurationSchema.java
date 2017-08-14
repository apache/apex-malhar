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
package org.apache.apex.malhar.lib.appdata.schemas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.dimensions.CustomTimeBucketRegistry;
import org.apache.apex.malhar.lib.dimensions.DimensionsDescriptor;
import org.apache.apex.malhar.lib.dimensions.aggregator.AbstractCompositeAggregator;
import org.apache.apex.malhar.lib.dimensions.aggregator.AbstractTopBottomAggregator;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorRegistry;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorUtils;
import org.apache.apex.malhar.lib.dimensions.aggregator.CompositeAggregator;
import org.apache.apex.malhar.lib.dimensions.aggregator.CompositeAggregatorFactory;
import org.apache.apex.malhar.lib.dimensions.aggregator.DefaultCompositeAggregatorFactory;
import org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator;
import org.apache.apex.malhar.lib.dimensions.aggregator.OTFAggregator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * <p>
 * This is a configuration schema which defines the configuration for any dimensions computation operation.
 * Users of this configuration schema include the
 * {@link org.apache.apex.malhar.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema}
 * operator as well as the {@link DimensionalSchema}, which represents dimensions information for App Data. The
 * schema is created by defining a configuration JSON schema like the example provided below.
 * The information from the JSON schema, is extracted and used to create metadata which is required by
 * libraries and operators related to dimesions computation.
 * </p>
 * <br/>
 * <br/>
 * Example schema:
 * <br/>
 * <br/>
 * {@code
 * {"keys":
 * [{"name":"keyName1","type":"type1"},
 * {"name":"keyName2","type":"type2"}],
 * "timeBuckets":["1m","1h","1d"],
 * "values":
 * [{"name":"valueName1","type":"type1","aggregators":["SUM"]},
 * {"name":"valueName2","type":"type2","aggregators":["SUM"]}]
 * "dimensions":
 * [{"combination":["keyName1","keyName2"],"additionalValues":["valueName1:MIN","valueName1:MAX","valueName2:MIN"]},
 * {"combination":["keyName1"],"additionalValues":["valueName1:MAX"]}]
 * }
 * }
 * <br/>
 * <br/>
 * <p>
 * The meta data that is built from this schema information is a set of maps. The two main maps are maps which define
 * key field descriptors and value field descriptors. These maps help to retrieve the correct {@link FieldsDescriptor}s
 * for different dimension combinations and different aggregations. The core components of these maps are the
 * dimensionsDescriptorID, and aggregatorID. The aggregatorID is just the ID assigned to an aggregator by the
 * aggregatorRegistry set on the configuration schema. The dimensionsDescriptorID is determined by the following method.
 * <br/>
 * <ol>
 * <li>The combinations defined in the dimensions section of the JSON schema are looped through in the order that they
 * are defined in the JSON.</li>
 * <li>For each combination, the time buckets are looped through in the order that they are defined.</li>
 * <li>The current combination and time bucket are combined to create a {@link DimensionsDescriptor}. The
 * dimensionsDescriptor is assigned an id and the id is incremented.</li>
 * </ol>
 * </p>
 * <p>
 * Below is a summary of the most important metadata and how it's structured.
 * </p>
 * <ul>
 * <li><b>dimensionsDescriptorIDToKeyDescriptor:</b> This is a map from a dimensionsDescriptor id to a key
 * {@link FieldsDescriptor}. The key {@link FieldsDescriptor} contains all of the key information for the dimensions
 * combination with the specified id.</li>
 * <li><b>dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor:</b> This is a map from a dimensionsDescriptor
 * ID to an aggregator id to a {@link FieldsDescriptor} for input values. This map is used to describe the name and
 * types of aggregates which are being aggregated with a particular aggregator under a particular dimension
 * combination.</li>
 * <li><b>dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor:</b> This is a map from a
 * dimensionsDescriptor ID to an aggregator id to a {@link FieldsDescriptor} for aggregates after aggregation. This
 * map is used to describe the name and output types of aggregates which are being aggregated with a particular
 * aggregator under a particular dimension combination. This map differs from the one described above because the
 * field descriptors in this map take into account changes in data types after aggregate values get aggregated.
 * For example if an average aggregation is applied to integers, then the output will be a float.</li>
 * </ul>
 *
 * @since 3.1.0
 */
public class DimensionalConfigurationSchema
{
  public static final int STARTING_TIMEBUCKET_ID = 256;
  /**
   * This is the separator that is used between a value field and the aggregator applied to that field. The
   * combined value is called an additional value and looks something like this cost:SUM.
   */
  public static final String ADDITIONAL_VALUE_SEPERATOR = ":";
  /**
   * The number of components in an additional value description.
   */
  public static final int ADDITIONAL_VALUE_NUM_COMPONENTS = 2;
  /**
   * The index of the field in an additional value description.
   */
  public static final int ADDITIONAL_VALUE_VALUE_INDEX = 0;
  /**
   * The index of the aggregator in an additional value description.
   */
  public static final int ADDITIONAL_VALUE_AGGREGATOR_INDEX = 1;
  /**
   * JSON key string for the keys section of the schema.
   */
  public static final String FIELD_KEYS = "keys";
  /**
   * The JSON key string for the name of a key.
   */
  public static final String FIELD_KEYS_NAME = "name";
  /**
   * The JSON key string for the type of a key.
   */
  public static final String FIELD_KEYS_TYPE = "type";
  /**
   * The JSON key string for the expression of a key. it is optional
   */
  public static final String FIELD_KEYS_EXPRESSION = "expression";
  /**
   * The JSON key string for the enumValues of a key.
   */
  public static final String FIELD_KEYS_ENUMVALUES = "enumValues";
  /**
   * A list of valid sets of JSON keys within a key JSON object. This is used to validate input data.
   */
  public static final List<Fields> VALID_KEY_FIELDS = ImmutableList.of(new Fields(Sets.newHashSet(FIELD_KEYS_NAME,
      FIELD_KEYS_TYPE,
      FIELD_KEYS_ENUMVALUES)),
      new Fields(Sets.newHashSet(FIELD_KEYS_NAME,
      FIELD_KEYS_TYPE)));
  /**
   * The JSON key string for the timeBuckets section of the schema.
   */
  public static final String FIELD_TIME_BUCKETS = "timeBuckets";
  /**
   * The JSON key string for the values section of the schema.
   */
  public static final String FIELD_VALUES = "values";
  /**
   * The JSON key string for the name of a value.
   */
  public static final String FIELD_VALUES_NAME = "name";
  /**
   * The JSON key string for the type of a value.
   */
  public static final String FIELD_VALUES_TYPE = "type";
  /**
   * The JSON key string for the type of a value.
   */
  public static final String FIELD_VALUES_EXPRESSION = "expression";

  /**
   * The JSON key string for the aggregators applied to a value accross all dimension combinations.
   */
  public static final String FIELD_VALUES_AGGREGATIONS = "aggregators";
  /**
   * The JSON key string used to identify the tags.
   */
  //TODO To be removed when Malhar Library 3.3 becomes a dependency.
  private static final String FIELD_TAGS = "tags";

  public static final String FIELD_VALUES_AGGREGATOR = "aggregator";
  //  public static final String FIELD_VALUES_AGGREGATOR_PROPERTY = "property";
  //  public static final String FIELD_VALUES_AGGREGATOR_PROPERTY_VALUE = "value";

  public static final String PROPERTY_NAME_EMBEDED_AGGREGATOR = "embededAggregator";
  public static final String PROPERTY_NAME_COUNT = "count";
  public static final String PROPERTY_NAME_SUB_COMBINATIONS = "subCombinations";

  /**
   * The JSON key string for the dimensions section of the schema.
   */
  public static final String FIELD_DIMENSIONS = "dimensions";
  public static final String FIELD_DIMENSIONS_ALL_COMBINATIONS = "ALL_COMBINATIONS";
  /**
   * The JSON key string for the combination subsection of the schema.
   */
  public static final String FIELD_DIMENSIONS_COMBINATIONS = "combination";
  /**
   * The JSON key string for the additional values subsection of the schema.
   */
  public static final String FIELD_DIMENSIONS_ADDITIONAL_VALUES = "additionalValues";
  /**
   * The JSON Key string for the timeBuckets defined on a per dimension combination basis.
   */
  public static final String FIELD_DIMENSIONS_TIME_BUCKETS = FIELD_TIME_BUCKETS;
  /**
   * This is a {@link FieldsDescriptor} object responsible for managing the key names and types.
   */
  private FieldsDescriptor keyDescriptor;
  private FieldsDescriptor keyDescriptorWithTime;
  /**
   * This is a {@link FieldsDescriptor} object responsible for managing the name and types of of input values.
   */
  private FieldsDescriptor inputValuesDescriptor;
  /**
   * This map holds all the enum values defined for each key.
   */
  private Map<String, List<Object>> keysToEnumValuesList;
  /**
   * This list maps a dimensions descriptor id to a {@link FieldsDescriptor} object for the key fields
   * corresponding to that dimensions descriptor.
   */
  private List<FieldsDescriptor> dimensionsDescriptorIDToKeyDescriptor;
  /**
   * This is a map from dimensions descriptor id to {@link DimensionsDescriptor}.
   */
  private List<DimensionsDescriptor> dimensionsDescriptorIDToDimensionsDescriptor;
  /**
   * This is a map from a dimensions descriptor id to a value to the set of all aggregations performed
   * on that value under the dimensions combination corresponding to that dimensions descriptor id.
   */
  private List<Map<String, Set<String>>> dimensionsDescriptorIDToValueToAggregator;
  /**
   * This is a map from a dimensions descriptor id to a value to the set of all on the fly aggregations
   * performed on that value under the dimensions combination corresponding to that dimensions descriptor
   * id.
   */
  private List<Map<String, Set<String>>> dimensionsDescriptorIDToValueToOTFAggregator;
  /**
   * This is a map from a dimensions descriptor id to a value to the set of all composite aggregations performed
   * on that value under the dimensions combination corresponding to that dimensions descriptor id.
   * it includes the time bucket combination
   */
  private List<Map<String, Set<String>>> dimensionsDescriptorIDToValueToCompositeAggregator;

  /**
   * This is a map from a dimensions descriptor id to an aggregator to a {@link FieldsDescriptor} object.
   * This is used internally to build dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor and
   * dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor in the
   * {@link buildDimensionsDescriptorIDAggregatorIDMaps}
   */
  private List<Map<String, FieldsDescriptor>> dimensionsDescriptorIDToAggregatorToAggregateDescriptor;
  /**
   * This is a map from a dimensions descriptor id to an OTF aggregator to a {@link FieldsDescriptor} object.
   */
  private List<Map<String, FieldsDescriptor>> dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor;
  /**
   * This is a map from a dimensions descriptor id to an composite aggregator to a {@link FieldsDescriptor} object.
   */
  private List<Map<String, FieldsDescriptor>> dimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor;
  /**
   * This is a map from a {@link DimensionsDescriptor} to its corresponding dimensions descriptor ID.
   */
  private Map<DimensionsDescriptor, Integer> dimensionsDescriptorToID;
  /**
   * This is a map from a dimensions descriptor id to an aggregator id to a {@link FieldsDescriptor} for the
   * input aggregates before any aggregation is performed on them.
   */
  private List<Int2ObjectMap<FieldsDescriptor>> dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor;
  /**
   * This is a map from a dimensions descriptor id to an aggregator id to a {@link FieldsDescriptor} for the
   * input aggregates after an aggregation is performed on them.
   */
  private List<Int2ObjectMap<FieldsDescriptor>> dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor;
  /**
   * This is a map from the dimensions descriptor id to the list of all aggregations performed on that dimensions
   * descriptor id. This list in fact only keep ddID to Incremental Aggregator IDs
   */
  private List<IntArrayList> dimensionsDescriptorIDToIncrementalAggregatorIDs;

  private List<IntArrayList> dimensionsDescriptorIDToCompositeAggregatorIDs;
  /**
   * This is a map from the dimensions descriptor id to field to all the additional value aggregations
   * specified for the dimensions combination.
   */
  private List<Map<String, Set<String>>> dimensionsDescriptorIDToFieldToAggregatorAdditionalValues;
  /**
   * This is a map from dimensions descriptor ids to all the keys fields involved in the dimensions combination.
   * it doesn't includes the time bucket
   */
  private List<Fields> dimensionsDescriptorIDToKeys;
  /**
   * Keys section of the schema.
   */
  private String keysString;
  /**
   * The time buckets section of the schema.
   */
  private String bucketsString;
  /**
   * The collection of aggregators to use with this schema.
   */
  private AggregatorRegistry aggregatorRegistry;
  /**
   * The time buckets which this schema specifies aggregations to be performed over.
   */
  private List<TimeBucket> timeBuckets;
  /**
   * The custom time buckets which this schema specifies aggregations to be performed over.
   */
  private List<CustomTimeBucket> customTimeBuckets;
  /**
   * This is a map from a value field to aggregations defined on that value (in the values
   * section of the JSON) to the type of the value field after aggregation is performed on it.
   * Please note that this map only contains the aggregations that are defined in the values section
   * of the {@link DimensionalConfigurationSchema} not the aggregations defined in the additionalValuesSection.
   */
  private Map<String, Map<String, Type>> schemaAllValueToAggregatorToType;
  /**
   * A map from keys to the schema tags defined for each key.
   */
  private Map<String, List<String>> keyToTags;
  /**
   * A map from values to the schema tags defined for each value.
   */
  private Map<String, List<String>> valueToTags;
  /**
   * The schema tags defined for each schema.
   */
  private List<String> tags;

  /**
   * A map: aggregate name ==> { property name ==> property value}
   */
  protected Map<String, Map<String, String>> aggregatorToProperty;
  protected List<CustomTimeBucket> customTimeBucketsCombination;

  private CustomTimeBucketRegistry customTimeBucketRegistry;

  protected CompositeAggregatorFactory compositeAggregatorFactory = DefaultCompositeAggregatorFactory.defaultInst;

  protected static final String[] COMPOSITE_AGGREGATORS = new String[]{"TOPN", "BOTTOMN"};
  protected static final Map<String, Set<String>> aggregatorToPropertiesMap = Maps.newHashMap();

  static {
    Set<String> topBottomProperties =
        Sets.newHashSet(PROPERTY_NAME_COUNT, PROPERTY_NAME_EMBEDED_AGGREGATOR, PROPERTY_NAME_SUB_COMBINATIONS);
    aggregatorToPropertiesMap.put("TOPN", topBottomProperties);
    aggregatorToPropertiesMap.put("BOTTOMN", topBottomProperties);
  }

  /**
   * keep the key to expression
   */
  private Map<String, String> keyToExpression = Maps.newHashMap();

  /**
   * keep the aggregate value to expression
   */
  private Map<String, String> valueToExpression = Maps.newHashMap();

  /**
   * Constructor for serialization.
   */
  private DimensionalConfigurationSchema()
  {
    //For kryo
  }

  /**
   * Creates a configuration schema with the given keys, values, timebuckets, dimensions combinations,
   * and aggregators.
   *
   * @param keys                   The keys to use in the {@link DimensionalConfigurationSchema}.
   * @param values                 The values to use in the {@link DimensionalConfigurationSchema}.
   * @param timeBuckets            The time buckets to use in the schema.
   * @param dimensionsCombinations The dimensions combinations for the schema.
   * @param aggregatorRegistry     The aggregators to apply to this schema.
   */
  public DimensionalConfigurationSchema(List<Key> keys,
      List<Value> values,
      List<TimeBucket> timeBuckets,
      List<DimensionsCombination> dimensionsCombinations,
      AggregatorRegistry aggregatorRegistry)
  {
    setAggregatorRegistry(aggregatorRegistry);

    initialize(keys,
        values,
        timeBuckets,
        dimensionsCombinations);
  }

  /**
   * Builds a {@link DimensionalConfigurationSchema} from the given JSON with the
   * given aggregator registry.
   *
   * @param json               The JSON from which to build the configuration schema.
   * @param aggregatorRegistry The aggregators to apply to the schema.
   */
  public DimensionalConfigurationSchema(String json,
      AggregatorRegistry aggregatorRegistry)
  {
    setAggregatorRegistry(aggregatorRegistry);

    try {
      initialize(json);
    } catch (JSONException ex) {
      LOG.error("{}", ex);
      throw new IllegalArgumentException(ex);
    }
  }

  /**
   * This is a helper method which sets and validates the {@link AggregatorRegistry}.
   *
   * @param aggregatorRegistry The {@link AggregatorRegistry}.
   */
  private void setAggregatorRegistry(AggregatorRegistry aggregatorRegistry)
  {
    this.aggregatorRegistry = Preconditions.checkNotNull(aggregatorRegistry);
  }

  /**
   * Gets the {@link AggregatorRegistry} associated with this schema.
   *
   * @return The {@link AggregatorRegistry} associated with this schema.
   */
  public AggregatorRegistry getAggregatorRegistry()
  {
    return aggregatorRegistry;
  }

  /**
   * @param ddIDToValueToAggregator
   * @return a list of AggregatorToAggregateDescriptor. namely ddID to AggregatorToAggregateDescriptor
   */
  private List<Map<String, FieldsDescriptor>> computeAggregatorToAggregateDescriptor(
      List<Map<String, Set<String>>> ddIDToValueToAggregator)
  {
    List<Map<String, FieldsDescriptor>> tempDdIDToAggregatorToAggregateDescriptor = Lists.newArrayList();

    for (int ddID = 0;
        ddID < ddIDToValueToAggregator.size();
        ddID++) {
      Map<String, Set<String>> valueToAggregator = ddIDToValueToAggregator.get(ddID);
      Map<String, Set<String>> aggregatorToValues = Maps.newHashMap();

      for (Map.Entry<String, Set<String>> entry : valueToAggregator.entrySet()) {
        String value = entry.getKey();
        for (String aggregator : entry.getValue()) {
          Set<String> values = aggregatorToValues.get(aggregator);

          if (values == null) {
            values = Sets.newHashSet();
            aggregatorToValues.put(aggregator, values);
          }

          values.add(value);
        }
      }

      Map<String, FieldsDescriptor> aggregatorToValuesDescriptor = Maps.newHashMap();

      for (Map.Entry<String, Set<String>> entry : aggregatorToValues.entrySet()) {
        final String aggregatorName = entry.getKey();
        if (isCompositeAggregator(aggregatorName)) {
          //for composite aggregator, the input type and output type are different
          aggregatorToValuesDescriptor.put(aggregatorName,
              AggregatorUtils.getOutputFieldsDescriptor(inputValuesDescriptor.getSubset(new Fields(entry.getValue())),
              this.getCompositeAggregatorByName(aggregatorName)));
        } else {
          aggregatorToValuesDescriptor.put(
              aggregatorName,
              inputValuesDescriptor.getSubset(new Fields(entry.getValue())));
        }
      }

      tempDdIDToAggregatorToAggregateDescriptor.add(aggregatorToValuesDescriptor);
    }

    return tempDdIDToAggregatorToAggregateDescriptor;
  }

  /**
   * This is a helper method which initializes the metadata for the {@link DimensionalConfigurationSchema}.
   *
   * @param keys                   The key objects to use when creating this configuration schema.
   * @param values                 The value objects to use when creating this configuration schema.
   * @param timeBuckets            The time buckets to use when creating this configuration schema.
   * @param dimensionsCombinations The dimensionsCombinations to use when creating this configuration schema.
   */
  private void initialize(List<Key> keys,
      List<Value> values,
      List<TimeBucket> timeBuckets,
      List<DimensionsCombination> dimensionsCombinations)
  {
    tags = Lists.newArrayList();

    keyToTags = Maps.newHashMap();

    for (Key key : keys) {
      keyToTags.put(key.getName(), new ArrayList<String>());
    }

    valueToTags = Maps.newHashMap();

    for (Value value : values) {
      valueToTags.put(value.getName(), new ArrayList<String>());
    }

    //time buckets
    this.timeBuckets = timeBuckets;
    this.customTimeBuckets = new ArrayList<>();

    customTimeBucketRegistry = new CustomTimeBucketRegistry(STARTING_TIMEBUCKET_ID);

    for (TimeBucket timeBucket : timeBuckets) {
      CustomTimeBucket customTimeBucket = new CustomTimeBucket(timeBucket);
      customTimeBuckets.add(customTimeBucket);
      customTimeBucketRegistry.register(customTimeBucket, timeBucket.ordinal());
    }

    //Input aggregate values

    Map<String, Type> valueFieldToType = Maps.newHashMap();

    for (Value value : values) {
      valueFieldToType.put(value.getName(), value.getType());
    }

    inputValuesDescriptor = new FieldsDescriptor(valueFieldToType);

    //Input keys

    Map<String, Type> keyFieldToType = Maps.newHashMap();
    keysToEnumValuesList = Maps.newHashMap();

    for (Key key : keys) {
      keyFieldToType.put(key.getName(), key.getType());
      keysToEnumValuesList.put(key.getName(), key.getEnumValues());
    }

    keyDescriptor = new FieldsDescriptor(keyFieldToType);
    Map<String, Type> fieldToTypeWithTime = Maps.newHashMap(keyFieldToType);
    keyDescriptorWithTime = keyDescriptorWithTime(fieldToTypeWithTime,
        customTimeBuckets);

    //schemaAllValueToAggregatorToType
    schemaAllValueToAggregatorToType = Maps.newHashMap();
    Map<String, Set<String>> specificValueToAggregator = Maps.newHashMap();
    Map<String, Set<String>> specificValueToOTFAggregator = Maps.newHashMap();
    Map<String, Set<String>> allSpecificValueToAggregator = Maps.newHashMap();

    for (Value value : values) {
      String valueName = value.getName();
      Set<String> aggregators = value.getAggregators();

      Set<String> specificAggregatorSet = Sets.newHashSet();
      Set<String> allAggregatorSet = Sets.newHashSet();
      Set<String> otfAggregators = Sets.newHashSet();

      for (String aggregatorName : aggregators) {
        if (aggregatorRegistry.isIncrementalAggregator(aggregatorName)) {
          specificAggregatorSet.add(aggregatorName);
          allAggregatorSet.add(aggregatorName);
        } else {
          otfAggregators.add(aggregatorName);
          List<String> aggregatorNames = aggregatorRegistry.getOTFAggregatorToIncrementalAggregators().get(
              aggregatorName);
          specificAggregatorSet.addAll(aggregatorNames);
          allAggregatorSet.addAll(aggregatorNames);
          allAggregatorSet.add(aggregatorName);
        }
      }

      specificValueToOTFAggregator.put(valueName, otfAggregators);
      specificValueToAggregator.put(valueName, specificAggregatorSet);
      allSpecificValueToAggregator.put(valueName, allAggregatorSet);
    }

    for (Map.Entry<String, Set<String>> entry : allSpecificValueToAggregator.entrySet()) {
      String valueName = entry.getKey();
      Type inputType = inputValuesDescriptor.getType(valueName);
      Set<String> aggregators = entry.getValue();
      Map<String, Type> aggregatorToType = Maps.newHashMap();

      for (String aggregatorName : aggregators) {

        if (aggregatorRegistry.isIncrementalAggregator(aggregatorName)) {
          IncrementalAggregator aggregator = aggregatorRegistry.getNameToIncrementalAggregator().get(aggregatorName);

          aggregatorToType.put(aggregatorName, aggregator.getOutputType(inputType));
        } else {
          OTFAggregator otfAggregator = aggregatorRegistry.getNameToOTFAggregators().get(aggregatorName);

          aggregatorToType.put(aggregatorName, otfAggregator.getOutputType());
        }
      }

      schemaAllValueToAggregatorToType.put(valueName, aggregatorToType);
    }

    //ddID

    dimensionsDescriptorIDToDimensionsDescriptor = Lists.newArrayList();
    dimensionsDescriptorIDToKeyDescriptor = Lists.newArrayList();
    dimensionsDescriptorToID = Maps.newHashMap();
    dimensionsDescriptorIDToValueToAggregator = Lists.newArrayList();
    dimensionsDescriptorIDToValueToOTFAggregator = Lists.newArrayList();

    int ddID = 0;
    for (DimensionsCombination dimensionsCombination : dimensionsCombinations) {
      for (TimeBucket timeBucket : timeBuckets) {
        DimensionsDescriptor dd = new DimensionsDescriptor(timeBucket, dimensionsCombination.getFields());
        dimensionsDescriptorIDToDimensionsDescriptor.add(dd);
        dimensionsDescriptorIDToKeyDescriptor.add(dd.createFieldsDescriptor(keyDescriptor));
        dimensionsDescriptorToID.put(dd, ddID);

        Map<String, Set<String>> valueToAggregator = Maps.newHashMap();
        Map<String, Set<String>> valueToOTFAggregator = Maps.newHashMap();

        Map<String, Set<String>> tempValueToAggregator = dimensionsCombination.getValueToAggregators();

        for (Map.Entry<String, Set<String>> entry : tempValueToAggregator.entrySet()) {
          String value = entry.getKey();
          Set<String> staticAggregatorNames = Sets.newHashSet();
          Set<String> otfAggregatorNames = Sets.newHashSet();
          Set<String> aggregatorNames = entry.getValue();

          for (String aggregatorName : aggregatorNames) {
            if (!aggregatorRegistry.isAggregator(aggregatorName)) {
              throw new UnsupportedOperationException("The aggregator "
                  + aggregatorName
                  + " is not valid.");
            }

            if (aggregatorRegistry.isIncrementalAggregator(aggregatorName)) {
              staticAggregatorNames.add(aggregatorName);
            } else {
              staticAggregatorNames.addAll(
                  aggregatorRegistry.getOTFAggregatorToIncrementalAggregators().get(aggregatorName));
              otfAggregatorNames.add(aggregatorName);
            }
          }

          valueToAggregator.put(value, staticAggregatorNames);
          valueToOTFAggregator.put(value, otfAggregatorNames);
        }

        mergeMaps(valueToAggregator, specificValueToAggregator);
        mergeMaps(valueToOTFAggregator, specificValueToOTFAggregator);

        dimensionsDescriptorIDToValueToAggregator.add(valueToAggregator);
        dimensionsDescriptorIDToValueToOTFAggregator.add(valueToOTFAggregator);
        ddID++;
      }
    }

    for (Map<String, Set<String>> valueToAggregator : dimensionsDescriptorIDToValueToAggregator) {

      if (specificValueToAggregator.isEmpty()) {
        continue;
      }

      for (Map.Entry<String, Set<String>> entry : specificValueToAggregator.entrySet()) {
        String valueName = entry.getKey();
        Set<String> aggName = entry.getValue();

        if (aggName.isEmpty()) {
          continue;
        }

        Set<String> ddAggregatorSet = valueToAggregator.get(valueName);

        if (ddAggregatorSet == null) {
          ddAggregatorSet = Sets.newHashSet();
          valueToAggregator.put(valueName, ddAggregatorSet);
        }

        ddAggregatorSet.addAll(aggName);
      }
    }

    for (Map<String, Set<String>> valueToAggregator : dimensionsDescriptorIDToValueToOTFAggregator) {

      if (specificValueToOTFAggregator.isEmpty()) {
        continue;
      }

      for (Map.Entry<String, Set<String>> entry : specificValueToOTFAggregator.entrySet()) {
        String valueName = entry.getKey();
        Set<String> aggName = entry.getValue();

        if (aggName.isEmpty()) {
          continue;
        }

        Set<String> ddAggregatorSet = valueToAggregator.get(valueName);

        if (ddAggregatorSet == null) {
          ddAggregatorSet = Sets.newHashSet();
          valueToAggregator.put(valueName, ddAggregatorSet);
        }

        ddAggregatorSet.addAll(aggName);
      }
    }

    //ddIDToAggregatorToAggregateDescriptor

    dimensionsDescriptorIDToAggregatorToAggregateDescriptor = computeAggregatorToAggregateDescriptor(
        dimensionsDescriptorIDToValueToAggregator);
    dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor = computeAggregatorToAggregateDescriptor(
        dimensionsDescriptorIDToValueToOTFAggregator);

    //combination ID values

    dimensionsDescriptorIDToFieldToAggregatorAdditionalValues = Lists.newArrayList();
    dimensionsDescriptorIDToKeys = Lists.newArrayList();

    for (DimensionsCombination dimensionsCombination : dimensionsCombinations) {
      dimensionsDescriptorIDToFieldToAggregatorAdditionalValues.add(dimensionsCombination.getValueToAggregators());
      dimensionsDescriptorIDToKeys.add(dimensionsCombination.getFields());
    }

    //Build keyString

    JSONArray keyArray = new JSONArray();

    for (Key key : keys) {
      JSONObject jo = new JSONObject();

      try {
        jo.put(FIELD_KEYS_NAME, key.getName());
        jo.put(FIELD_KEYS_TYPE, key.getType().getName());

        JSONArray enumArray = new JSONArray();

        for (Object enumVal : key.getEnumValues()) {
          enumArray.put(enumVal);
        }

        jo.put(FIELD_KEYS_ENUMVALUES, enumArray);
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }

      keyArray.put(jo);
    }

    keysString = keyArray.toString();

    //Build time buckets

    JSONArray timeBucketArray = new JSONArray();

    for (CustomTimeBucket timeBucket : customTimeBuckets) {
      timeBucketArray.put(timeBucket.getText());
    }

    bucketsString = timeBucketArray.toString();

    //buildDDIDAggID

    //composite aggregator are not supported in this method. add empty list to avoid unit test error.
    dimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor = Lists.newArrayList();

    buildDimensionsDescriptorIDAggregatorIDMaps();
  }

  /**
   * This is a helper method which initializes the {@link DimensionalConfigurationSchema} with the given
   * JSON values.
   *
   * @param json The json with which to initialize the {@link DimensionalConfigurationSchema}.
   * @throws JSONException
   */
  private void initialize(String json) throws JSONException
  {
    JSONObject jo = new JSONObject(json);

    tags = getTags(jo);

    //Keys

    keysToEnumValuesList = Maps.newHashMap();
    JSONArray keysArray;

    if (jo.has(FIELD_KEYS)) {
      keysArray = jo.getJSONArray(FIELD_KEYS);
    } else {
      keysArray = new JSONArray();
    }

    keysString = keysArray.toString();
    keyToTags = Maps.newHashMap();

    Map<String, Type> fieldToType = Maps.newHashMap();

    for (int keyIndex = 0;
        keyIndex < keysArray.length();
        keyIndex++) {
      JSONObject tempKeyDescriptor = keysArray.getJSONObject(keyIndex);

      SchemaUtils.checkValidKeysEx(tempKeyDescriptor, VALID_KEY_FIELDS);

      String keyName = tempKeyDescriptor.getString(FIELD_KEYS_NAME);
      String typeName = tempKeyDescriptor.getString(FIELD_KEYS_TYPE);

      try {
        String keyExpression = tempKeyDescriptor.getString(FIELD_KEYS_EXPRESSION);
        if (keyExpression != null) {
          keyToExpression.put(keyName, keyExpression);
        }
      } catch (JSONException e) {
        //do nothing
      }

      List<String> keyTags = getTags(tempKeyDescriptor);

      keyToTags.put(keyName, keyTags);

      Type type = Type.getTypeEx(typeName);
      fieldToType.put(keyName, type);

      List<Object> valuesList = Lists.newArrayList();
      keysToEnumValuesList.put(keyName, valuesList);

      if (tempKeyDescriptor.has(FIELD_KEYS_ENUMVALUES)) {
        Type maxType = null;
        JSONArray valArray = tempKeyDescriptor.getJSONArray(FIELD_KEYS_ENUMVALUES);

        //Validate the provided data types
        for (int valIndex = 0;
            valIndex < valArray.length();
            valIndex++) {
          Object val = valArray.get(valIndex);
          valuesList.add(val);

          Preconditions.checkState(!(val instanceof JSONArray || val instanceof JSONObject),
              "The value must be a primitive.");

          Type currentType = Type.CLASS_TO_TYPE.get(val.getClass());

          if (maxType == null) {
            maxType = currentType;
          } else if (maxType != currentType) {
            if (maxType.getHigherTypes().contains(currentType)) {
              maxType = currentType;
            } else {
              Preconditions.checkState(currentType.getHigherTypes().contains(maxType),
                  "Conficting types: " + currentType.getName() + " cannot be converted to " + maxType.getName());
            }
          }
        }

        //This is not the right thing to do, fix later
        if (!Type.areRelated(maxType, type)) {
          throw new IllegalArgumentException("The type of the values in "
              + valArray + " is " + maxType.getName()
              + " while the specified type is " + type.getName());
        }
      }
    }

    //Time Buckets
    timeBuckets = Lists.newArrayList();
    customTimeBuckets = Lists.newArrayList();

    JSONArray timeBucketsJSON;

    if (!jo.has(FIELD_TIME_BUCKETS)) {
      timeBucketsJSON = new JSONArray();
      timeBucketsJSON.put(TimeBucket.ALL.getText());
    } else {
      timeBucketsJSON = jo.getJSONArray(FIELD_TIME_BUCKETS);

      if (timeBucketsJSON.length() == 0) {
        throw new IllegalArgumentException("A time bucket must be specified.");
      }
    }

    customTimeBucketRegistry = new CustomTimeBucketRegistry(STARTING_TIMEBUCKET_ID);

    Set<CustomTimeBucket> customTimeBucketsAllSet = Sets.newHashSet();
    List<CustomTimeBucket> customTimeBucketsAll = Lists.newArrayList();

    Set<CustomTimeBucket> customTimeBucketsTotalSet = Sets.newHashSet();

    for (int timeBucketIndex = 0;
        timeBucketIndex < timeBucketsJSON.length();
        timeBucketIndex++) {
      String timeBucketString = timeBucketsJSON.getString(timeBucketIndex);
      CustomTimeBucket customTimeBucket = new CustomTimeBucket(timeBucketString);

      if (!customTimeBucketsAllSet.add(customTimeBucket)) {
        throw new IllegalArgumentException("The bucket " + customTimeBucket.getText() + " was defined twice.");
      }

      customTimeBucketsAll.add(customTimeBucket);

      customTimeBuckets.add(customTimeBucket);

      if (customTimeBucket.isUnit() || customTimeBucket.getTimeBucket() == TimeBucket.ALL) {
        timeBuckets.add(customTimeBucket.getTimeBucket());
      }
    }

    customTimeBucketsTotalSet.addAll(customTimeBucketsAllSet);

    JSONArray customTimeBucketsJSON = new JSONArray();

    for (CustomTimeBucket customTimeBucket : customTimeBuckets) {
      customTimeBucketsJSON.put(customTimeBucket.toString());
    }

    bucketsString = customTimeBucketsJSON.toString();

    //Key descriptor all
    keyDescriptor = new FieldsDescriptor(fieldToType);

    Map<String, Type> fieldToTypeWithTime = Maps.newHashMap(fieldToType);
    keyDescriptorWithTime = keyDescriptorWithTime(fieldToTypeWithTime,
        customTimeBuckets);

    //Values

    Map<String, Set<String>> allValueToAggregator = Maps.newHashMap();
    Map<String, Set<String>> allValueToOTFAggregator = Maps.newHashMap();
    Map<String, Set<String>> valueToAggregators = Maps.newHashMap();
    Map<String, Set<String>> valueToOTFAggregators = Maps.newHashMap();
    Map<String, Set<String>> allValueToCompositeAggregator = Maps.newHashMap();
    Map<String, Set<String>> valueToCompositeAggregators = Maps.newHashMap();

    Map<String, Type> aggFieldToType = Maps.newHashMap();
    JSONArray valuesArray = jo.getJSONArray(FIELD_VALUES);
    schemaAllValueToAggregatorToType = Maps.newHashMap();
    valueToTags = Maps.newHashMap();

    for (int valueIndex = 0;
        valueIndex < valuesArray.length();
        valueIndex++) {
      JSONObject value = valuesArray.getJSONObject(valueIndex);
      String name = value.getString(FIELD_VALUES_NAME);
      String type = value.getString(FIELD_VALUES_TYPE);

      try {
        String valueExpression = value.getString(FIELD_VALUES_EXPRESSION);
        if (valueExpression != null) {
          valueToExpression.put(name, valueExpression);
        }
      } catch (JSONException e) {
        //Do nothing
      }

      List<String> valueTags = getTags(value);

      valueToTags.put(name, valueTags);

      Type typeT = Type.NAME_TO_TYPE.get(type);

      if (aggFieldToType.containsKey(name)) {
        throw new IllegalArgumentException("Cannot define the value " + name +
            " twice.");
      }

      Map<String, Type> aggregatorToType = Maps.newHashMap();
      schemaAllValueToAggregatorToType.put(name, aggregatorToType);

      aggFieldToType.put(name, typeT);
      Set<String> aggregatorSet = Sets.newHashSet();
      Set<String> aggregatorOTFSet = Sets.newHashSet();
      Set<String> aggregateCompositeSet = Sets.newHashSet();

      if (value.has(FIELD_VALUES_AGGREGATIONS)) {
        JSONArray aggregators = value.getJSONArray(FIELD_VALUES_AGGREGATIONS);

        if (aggregators.length() == 0) {
          throw new IllegalArgumentException("Empty aggregators array for: " + name);
        }

        for (int aggregatorIndex = 0;
            aggregatorIndex < aggregators.length();
            aggregatorIndex++) {

          //the aggrator is not only has name any more, it could be an object or a String
          //example: {"aggregator":"BOTTOMN","property":"count","value":"20","embededAggregator":"AVG"}
          String aggregatorType = null;
          aggregatorType = aggregators.getString(aggregatorIndex);
          if (isJsonSimpleString(aggregatorType)) {
            //it's is simple aggregator
            addNonCompositeAggregator(aggregatorType, allValueToAggregator, allValueToOTFAggregator,
                name, aggregatorSet, aggregatorToType, typeT, aggregatorOTFSet, true);
          } else {
            //it is a composite aggragate
            JSONObject jsonAggregator = aggregators.getJSONObject(aggregatorIndex);
            aggregatorType = jsonAggregator.getString(FIELD_VALUES_AGGREGATOR);
            Map<String, Object> propertyNameToValue = getPropertyNameToValue(jsonAggregator, aggregatorType);

            //the steps following is for composite aggregator.
            if (isCompositeAggregator(aggregatorType)) {
              String embededAggregatorName = (String)propertyNameToValue.get(PROPERTY_NAME_EMBEDED_AGGREGATOR);

              /**
               * don't add embed aggregator here as the emebed aggregator is with different dimension as this dimension
               * maybe haven't created yet.
               */
              CompositeAggregator aggregator = addCompositeAggregator(aggregatorType, allValueToCompositeAggregator,
                  aggregateCompositeSet, name,
                  embededAggregatorName, propertyNameToValue, aggregatorToType);

            } else {
              throw new IllegalArgumentException(
                  "Unknow aggregator type: " + aggregatorType + ", please check if it valid.");
            }

          }
        }
      }

      if (!aggregatorSet.isEmpty()) {
        valueToAggregators.put(name, aggregatorSet);
        valueToOTFAggregators.put(name, aggregatorOTFSet);
      }
      if (!aggregateCompositeSet.isEmpty()) {
        valueToCompositeAggregators.put(name, aggregateCompositeSet);
      }
    }

    LOG.debug("allValueToAggregator {}", allValueToAggregator);
    LOG.debug("valueToAggregators {}", valueToAggregators);

    this.inputValuesDescriptor = new FieldsDescriptor(aggFieldToType);

    // Dimensions

    dimensionsDescriptorIDToValueToAggregator = Lists.newArrayList();
    dimensionsDescriptorIDToValueToOTFAggregator = Lists.newArrayList();
    dimensionsDescriptorIDToValueToCompositeAggregator = Lists.newArrayList();

    dimensionsDescriptorIDToKeyDescriptor = Lists.newArrayList();
    dimensionsDescriptorIDToDimensionsDescriptor = Lists.newArrayList();
    dimensionsDescriptorIDToAggregatorToAggregateDescriptor = Lists.newArrayList();

    dimensionsDescriptorIDToKeys = Lists.newArrayList();
    dimensionsDescriptorIDToFieldToAggregatorAdditionalValues = Lists.newArrayList();

    JSONArray dimensionsArray;

    if (jo.has(FIELD_DIMENSIONS)) {
      Object dimensionsVal = jo.get(FIELD_DIMENSIONS);

      if (dimensionsVal instanceof String) {
        if (!((String)dimensionsVal).equals(FIELD_DIMENSIONS_ALL_COMBINATIONS)) {
          throw new IllegalArgumentException(dimensionsVal + " is an invalid value for " + FIELD_DIMENSIONS);
        }

        dimensionsArray = new JSONArray();

        LOG.debug("Combinations size {}", fieldToType.keySet().size());
        Set<Set<String>> combinations = buildCombinations(fieldToType.keySet());
        LOG.debug("Combinations size {}", combinations.size());
        List<DimensionsDescriptor> dimensionDescriptors = Lists.newArrayList();

        for (Set<String> combination : combinations) {
          dimensionDescriptors.add(new DimensionsDescriptor(new Fields(combination)));
        }

        Collections.sort(dimensionDescriptors);
        LOG.debug("Dimensions descriptor size {}", dimensionDescriptors.size());

        for (DimensionsDescriptor dimensionsDescriptor : dimensionDescriptors) {
          JSONObject combination = new JSONObject();
          JSONArray combinationKeys = new JSONArray();

          for (String field : dimensionsDescriptor.getFields().getFields()) {
            combinationKeys.put(field);
          }

          combination.put(FIELD_DIMENSIONS_COMBINATIONS, combinationKeys);
          dimensionsArray.put(combination);
        }
      } else if (dimensionsVal instanceof JSONArray) {
        dimensionsArray = jo.getJSONArray(FIELD_DIMENSIONS);
      } else {
        throw new IllegalArgumentException("The value for " + FIELD_DIMENSIONS + " must be a string or an array.");
      }
    } else {
      dimensionsArray = new JSONArray();
      JSONObject combination = new JSONObject();
      combination.put(FIELD_DIMENSIONS_COMBINATIONS, new JSONArray());
      dimensionsArray.put(combination);
    }

    Set<Fields> dimensionsDescriptorFields = Sets.newHashSet();

    //loop through dimension descriptors
    for (int dimensionsIndex = 0;
        dimensionsIndex < dimensionsArray.length();
        dimensionsIndex++) {
      //Get a dimension descriptor
      JSONObject dimension = dimensionsArray.getJSONObject(dimensionsIndex);
      //Get the key fields of a descriptor
      JSONArray combinationFields = dimension.getJSONArray(FIELD_DIMENSIONS_COMBINATIONS);

      //valueName to IncrementalAggregator
      Map<String, Set<String>> specificValueToAggregator = Maps.newHashMap();
      //valueName to OTFAggregator
      Map<String, Set<String>> specificValueToOTFAggregator = Maps.newHashMap();
      //valueName to CompositeAggregator
      Map<String, Set<String>> specificValueToCompositeAggregator = Maps.newHashMap();
      //TODO: need a mechanism to check the value name is value.

      for (Map.Entry<String, Set<String>> entry : valueToAggregators.entrySet()) {
        Set<String> aggregators = Sets.newHashSet();
        aggregators.addAll(entry.getValue());
        specificValueToAggregator.put(entry.getKey(), aggregators);
      }

      for (Map.Entry<String, Set<String>> entry : valueToOTFAggregators.entrySet()) {
        Set<String> aggregators = Sets.newHashSet();
        aggregators.addAll(entry.getValue());
        specificValueToOTFAggregator.put(entry.getKey(), aggregators);
      }

      for (Map.Entry<String, Set<String>> entry : valueToCompositeAggregators.entrySet()) {
        Set<String> aggregators = Sets.newHashSet();
        aggregators.addAll(entry.getValue());
        specificValueToCompositeAggregator.put(entry.getKey(), aggregators);
      }

      List<String> keyNames = Lists.newArrayList();
      //loop through the key fields of a descriptor
      for (int keyIndex = 0;
          keyIndex < combinationFields.length();
          keyIndex++) {
        String keyName = combinationFields.getString(keyIndex);
        keyNames.add(keyName);
      }

      Fields dimensionDescriptorFields = new Fields(keyNames);

      if (!dimensionsDescriptorFields.add(dimensionDescriptorFields)) {
        throw new IllegalArgumentException("Duplicate dimension descriptor: " +
            dimensionDescriptorFields);
      }

      Map<String, Set<String>> fieldToAggregatorAdditionalValues = Maps.newHashMap();
      dimensionsDescriptorIDToKeys.add(dimensionDescriptorFields);
      dimensionsDescriptorIDToFieldToAggregatorAdditionalValues.add(fieldToAggregatorAdditionalValues);

      Set<CustomTimeBucket> customTimeBucketsCombinationSet = Sets.newHashSet(customTimeBucketsAllSet);
      customTimeBucketsCombination = Lists.newArrayList(customTimeBucketsAll);

      if (dimension.has(DimensionalConfigurationSchema.FIELD_DIMENSIONS_TIME_BUCKETS)) {
        JSONArray timeBuckets = dimension.getJSONArray(DimensionalConfigurationSchema.FIELD_DIMENSIONS_TIME_BUCKETS);

        if (timeBuckets.length() == 0) {
          throw new IllegalArgumentException(dimensionDescriptorFields.getFields().toString());
        }

        for (int timeBucketIndex = 0; timeBucketIndex < timeBuckets.length(); timeBucketIndex++) {
          CustomTimeBucket customTimeBucket = new CustomTimeBucket(timeBuckets.getString(timeBucketIndex));

          if (!customTimeBucketsCombinationSet.add(customTimeBucket)) {
            throw new IllegalArgumentException("The time bucket " +
                customTimeBucket +
                " is defined twice for the dimensions combination " +
                dimensionDescriptorFields.getFields().toString());
          }

          customTimeBucketsCombinationSet.add(customTimeBucket);
          customTimeBucketsCombination.add(customTimeBucket);
        }
      }

      customTimeBucketsTotalSet.addAll(customTimeBucketsCombinationSet);

      //Loop through time to generate dimension descriptors
      for (CustomTimeBucket timeBucket : customTimeBucketsCombination) {
        DimensionsDescriptor dimensionsDescriptor = new DimensionsDescriptor(timeBucket, dimensionDescriptorFields);
        dimensionsDescriptorIDToKeyDescriptor.add(dimensionsDescriptor.createFieldsDescriptor(keyDescriptor));
        dimensionsDescriptorIDToDimensionsDescriptor.add(dimensionsDescriptor);
      }

      if (dimension.has(FIELD_DIMENSIONS_ADDITIONAL_VALUES)) {
        JSONArray additionalValues = dimension.getJSONArray(FIELD_DIMENSIONS_ADDITIONAL_VALUES);

        //iterate over additional values
        for (int additionalValueIndex = 0;
            additionalValueIndex < additionalValues.length();
            additionalValueIndex++) {
          String additionalValue = additionalValues.getString(additionalValueIndex);

          if (isJsonSimpleString(additionalValue)) {
            String[] components = additionalValue.split(ADDITIONAL_VALUE_SEPERATOR);

            if (components.length != ADDITIONAL_VALUE_NUM_COMPONENTS) {
              throw new IllegalArgumentException("The number of component values "
                  + "in an additional value must be "
                  + ADDITIONAL_VALUE_NUM_COMPONENTS
                  + " not " + components.length);
            }

            String valueName = components[ADDITIONAL_VALUE_VALUE_INDEX];
            verifyValueDefined(valueName, aggFieldToType.keySet());

            String aggregatorName = components[ADDITIONAL_VALUE_AGGREGATOR_INDEX];

            {
              Set<String> aggregators = fieldToAggregatorAdditionalValues.get(valueName);

              if (aggregators == null) {
                aggregators = Sets.newHashSet();
                fieldToAggregatorAdditionalValues.put(valueName, aggregators);
              }

              aggregators.add(aggregatorName);
            }

            if (!aggregatorRegistry.isAggregator(aggregatorName)) {
              throw new IllegalArgumentException(aggregatorName + " is not a valid aggregator.");
            }

            if (aggregatorRegistry.isIncrementalAggregator(aggregatorName)) {
              Set<String> aggregatorNames = allValueToAggregator.get(valueName);

              if (aggregatorNames == null) {
                aggregatorNames = Sets.newHashSet();
                allValueToAggregator.put(valueName, aggregatorNames);
              }

              aggregatorNames.add(aggregatorName);

              Set<String> aggregators = specificValueToAggregator.get(valueName);
              if (aggregators == null) {
                aggregators = Sets.newHashSet();
                specificValueToAggregator.put(valueName, aggregators);
              }

              if (!aggregators.add(aggregatorName)) {
                throw new IllegalArgumentException("The aggregator " + aggregatorName
                    + " was already defined in the " + FIELD_VALUES
                    + " section for the value " + valueName);
              }
            } else {
              //Check for duplicate on the fly aggregators
              Set<String> aggregatorNames = specificValueToOTFAggregator.get(valueName);

              if (aggregatorNames == null) {
                aggregatorNames = Sets.newHashSet();
                specificValueToOTFAggregator.put(valueName, aggregatorNames);
              }

              if (!aggregatorNames.add(aggregatorName)) {
                throw new IllegalArgumentException("The aggregator " + aggregatorName +
                    " cannot be specified twice for the value " + valueName);
              }

              aggregatorNames = allValueToOTFAggregator.get(valueName);

              if (aggregatorNames == null) {
                aggregatorNames = Sets.newHashSet();
                allValueToOTFAggregator.put(valueName, aggregatorNames);
              }

              if (!aggregatorNames.add(aggregatorName)) {
                throw new IllegalArgumentException("The aggregator " + aggregatorName +
                    " cannot be specified twice for the value " + valueName);
              }

              Set<String> aggregators = specificValueToAggregator.get(valueName);

              if (aggregators == null) {
                aggregators = Sets.newHashSet();
                specificValueToAggregator.put(valueName, aggregators);
              }

              if (aggregators == null) {
                throw new IllegalArgumentException("The additional value " + additionalValue
                    + "Does not have a corresponding value " + valueName
                    + " defined in the " + FIELD_VALUES + " section.");
              }

              aggregators.addAll(aggregatorRegistry.getOTFAggregatorToIncrementalAggregators().get(aggregatorName));
            }
          } else {
            //it is a composite aggragate
            JSONObject jsonAddition = additionalValues.getJSONObject(additionalValueIndex);
            String valueName = (String)jsonAddition.keys().next();
            verifyValueDefined(valueName, aggFieldToType.keySet());

            JSONObject jsonAggregator = jsonAddition.getJSONObject(valueName);
            String aggregatorName = jsonAggregator.getString(FIELD_VALUES_AGGREGATOR);
            Map<String, Object> propertyNameToValue = getPropertyNameToValue(jsonAggregator, aggregatorName);

            //the steps following is for composite aggregator.
            if (isCompositeAggregator(aggregatorName)) {
              String embededAggregatorName = (String)propertyNameToValue.get(PROPERTY_NAME_EMBEDED_AGGREGATOR);

              /**
               * don't add embed aggregator here as the emebed aggregator is with different dimension as this dimension
               * maybe haven't created yet. the subCombination should be part of the combination
               */
              Set<String> compositeAggregators = specificValueToCompositeAggregator.get(valueName);
              if (compositeAggregators == null) {
                compositeAggregators = Sets.newHashSet();
                specificValueToCompositeAggregator.put(valueName, compositeAggregators);
              }
              CompositeAggregator aggregator = addCompositeAggregator(aggregatorName, allValueToCompositeAggregator,
                  compositeAggregators,
                  valueName, embededAggregatorName, propertyNameToValue, null);
            } else {
              throw new IllegalArgumentException(
                  "Unknow aggregator name: " + aggregatorName + ", please check if it valid.");
            }
          }

        }
      }

      if (specificValueToAggregator.isEmpty() && specificValueToCompositeAggregator.isEmpty()) {
        throw new IllegalArgumentException("No aggregations defined for the " +
            "following field combination " +
            combinationFields.toString());
      }

      for (CustomTimeBucket customTimeBucket : customTimeBucketsCombination) {
        dimensionsDescriptorIDToValueToAggregator.add(specificValueToAggregator);
        dimensionsDescriptorIDToValueToOTFAggregator.add(specificValueToOTFAggregator);
        dimensionsDescriptorIDToValueToCompositeAggregator.add(specificValueToCompositeAggregator);
      }
    }

    customTimeBucketsAll.clear();
    customTimeBucketsAll.addAll(customTimeBucketsTotalSet);
    Collections.sort(customTimeBucketsAll);

    for (CustomTimeBucket customTimeBucket : customTimeBucketsAll) {
      if (customTimeBucketRegistry.getTimeBucketId(customTimeBucket) == null) {
        if (customTimeBucket.isUnit() || customTimeBucket.getTimeBucket() == TimeBucket.ALL) {
          customTimeBucketRegistry.register(customTimeBucket, customTimeBucket.getTimeBucket().ordinal());
        } else {
          customTimeBucketRegistry.register(customTimeBucket);
        }
      }
    }

    //compute addition dimension and aggregator for composite aggregator
    computeAdditionalDimensionForCompositeAggregators();

    //DD ID To Aggregator To Aggregate Descriptor
    dimensionsDescriptorIDToAggregatorToAggregateDescriptor = computeAggregatorToAggregateDescriptor(
        dimensionsDescriptorIDToValueToAggregator);

    //DD ID To OTF Aggregator To Aggregator Descriptor
    dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor = computeAggregatorToAggregateDescriptor(
        dimensionsDescriptorIDToValueToOTFAggregator);

    dimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor = computeAggregatorToAggregateDescriptor(
        dimensionsDescriptorIDToValueToCompositeAggregator);

    //Dimensions Descriptor To ID
    dimensionsDescriptorToID = Maps.newHashMap();

    for (int index = 0;
        index < dimensionsDescriptorIDToDimensionsDescriptor.size();
        index++) {
      dimensionsDescriptorToID.put(dimensionsDescriptorIDToDimensionsDescriptor.get(index), index);
    }

    //Build id maps
    buildDimensionsDescriptorIDAggregatorIDMaps();

    aggregatorRegistry.buildTopBottomAggregatorIDToAggregator();

    //fulfill the embed ddids of composite aggregators
    fulfillCompositeAggregatorExtraInfo();
  }

  protected Map<String, Object> getPropertyNameToValue(JSONObject jsonAggregator, String aggregatorName)
      throws JSONException
  {
    //aggregatorName = jsonAggregator.getString(FIELD_VALUES_AGGREGATOR);

    Set<String> propertyNames = aggregatorToPropertiesMap.get(aggregatorName);

    if (propertyNames == null) {
      return Collections.emptyMap();
    }

    Map<String, Object> propertyNameToValue = Maps.newHashMap();
    for (String propertyName : propertyNames) {
      String propertyValue = jsonAggregator.getString(propertyName);
      if (propertyValue == null) {
        continue;
      }

      if (isJsonSimpleString(propertyValue)) {
        propertyNameToValue.put(propertyName, propertyValue);
      } else {
        JSONArray propertyValues = jsonAggregator.getJSONArray(propertyName);
        if (propertyValues != null) {
          final int valueLength = propertyValues.length();
          String[] values = new String[valueLength];
          for (int i = 0; i < valueLength; ++i) {
            values[i] = propertyValues.getString(i);
          }
          propertyNameToValue.put(propertyName, values);
        }
      }
    }
    return propertyNameToValue;
  }

  /**
   * The composite aggregator is not only aggregator type.
   *
   * @param aggregatorName
   * @return
   */
  protected boolean isCompositeAggregator(String aggregatorName)
  {
    aggregatorName = aggregatorName.split("-")[0];
    for (int index = 0; index < COMPOSITE_AGGREGATORS.length; ++index) {
      if (COMPOSITE_AGGREGATORS[index].equals(aggregatorName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * This is a helper method which converts the given {@link JSONArray} to a {@link List} of Strings.
   *
   * @param jsonStringArray The {@link JSONArray} to convert.
   * @return The converted {@link List} of Strings.
   */
  //TODO To be removed when Malhar Library 3.3 becomes a dependency.
  private List<String> getStringsFromJSONArray(JSONArray jsonStringArray) throws JSONException
  {
    List<String> stringArray = Lists.newArrayListWithCapacity(jsonStringArray.length());

    for (int stringIndex = 0; stringIndex < jsonStringArray.length(); stringIndex++) {
      stringArray.add(jsonStringArray.getString(stringIndex));
    }

    return stringArray;
  }

  /**
   * The composite aggregators could add additional dimensions and aggregators.
   * This method parse all composite aggregators to genereate additional dimensions and aggregators(embeded
   * aggregators).
   */
  protected void computeAdditionalDimensionForCompositeAggregators()
  {
    //NOTES: dimensionsDescriptorIDToKeys doesn't have time bucket combination while
    // dimensionsDescriptorIDToValueToCompositeAggregator has.
    //the fieldsCombinations generated just check if the keys already existed.
    Map<Set<String>, Integer> keysToCombinationId = getKeysToCombinationId();

    //the dimensionsDescriptorIDToKeys will be change during the process, only to go through the initial for
    // composite aggregator.
    final int initialKeysCombinationsSize = dimensionsDescriptorIDToKeys.size();
    for (int keysIndex = 0; keysIndex < initialKeysCombinationsSize; ++keysIndex) {
      Set<String> keys = dimensionsDescriptorIDToKeys.get(keysIndex).getFields();

      int ddId = keysIndex * customTimeBucketsCombination.size();
      Map<String, Set<String>> valueToAggregators = dimensionsDescriptorIDToValueToCompositeAggregator.get(ddId);
      Map<String, Set<String>> compositeAggregatorToValues = getAggregatorToValues(valueToAggregators);

      // for the embed aggregator, the value is composite's value; the key is composite's key combined with
      // subCombination
      for (Map.Entry<String, Set<String>> aggregatorToValuesEntry : compositeAggregatorToValues.entrySet()) {
        AbstractTopBottomAggregator aggregator = getCompositeAggregatorByName(aggregatorToValuesEntry.getKey());
        Set<String> subCombination = aggregator.getSubCombinations();
        addSubKeysAndAggregator(aggregatorToValuesEntry.getValue(), keys, subCombination,
            aggregator.getEmbedAggregatorName(), keysToCombinationId);
      }
    }
  }

  protected Map<String, Set<String>> getAggregatorToValues(Map<String, Set<String>> valueToAggregators)
  {
    Map<String, Set<String>> aggregatorToValues = Maps.newHashMap();
    for (Map.Entry<String, Set<String>> entry : valueToAggregators.entrySet()) {
      for (String aggregator : entry.getValue()) {
        Set<String> values = aggregatorToValues.get(aggregator);
        if (values == null) {
          values = Sets.newHashSet();
          aggregatorToValues.put(aggregator, values);
        }
        values.add(entry.getKey());
      }
    }
    return aggregatorToValues;
  }

  protected AbstractTopBottomAggregator getCompositeAggregatorByName(String compositeAggregatorName)
  {
    return aggregatorRegistry.getNameToTopBottomAggregator().get(compositeAggregatorName);
  }

  /**
   * NOTES: dimensionsDescriptorIDToKeys doesn't have time bucket combination, so one key set should only index
   *
   * @return map from field keys to CombinationId
   */
  protected Map<Set<String>, Integer> getKeysToCombinationId()
  {
    Map<Set<String>, Integer> keysToDdid = Maps.newHashMap();
    for (int index = 0; index < dimensionsDescriptorIDToKeys.size(); ++index) {
      Set<String> keys = Sets.newHashSet();
      keys.addAll(dimensionsDescriptorIDToKeys.get(index).getFieldsList());

      Integer orgIndex = keysToDdid.put(keys, index);
      if (orgIndex != null) {
        throw new RuntimeException("The keys" + keys + "already have a index " + index + " associated with it.");
      }
    }
    return keysToDdid;
  }

  /**
   * add sub-keys and aggregtor to the dimension description.
   * add the aggregator to the dimension if it already existed, else add new dimension
   * precondition: neither keys nor subKeys are empty. Keys empty means all keys, composite aggregator should not
   * apply to this case
   * NOTES: keep the data integration
   *
   * @param values
   * @param keysOfCompositeAggregator
   * @param subKeys
   * @param aggregatorName            the name of the aggregator. it should be an incremental aggregator only
   * @param keysToCombinationId       keep the fields combinations, don't include time bucket combination.
   */
  protected void addSubKeysAndAggregator(Set<String> values, Set<String> keysOfCompositeAggregator, Set<String> subKeys,
      String aggregatorName, Map<Set<String>, Integer> keysToCombinationId)
  {
    if (keysOfCompositeAggregator == null || subKeys == null || keysOfCompositeAggregator.isEmpty() ||
        subKeys.isEmpty()) {
      throw new IllegalArgumentException("Both keys and subKeys can't be null or empty");
    }

    Set<String> allKeys = Sets.newHashSet();
    allKeys.addAll(keysOfCompositeAggregator);
    allKeys.addAll(subKeys);
    if (allKeys.size() != keysOfCompositeAggregator.size() + subKeys.size()) {
      throw new IllegalArgumentException(
          "Should NOT have overlap between keys " + keysOfCompositeAggregator.toString() + " and subKeys " + subKeys);
    }

    Integer combinationId = keysToCombinationId.get(allKeys);
    if (combinationId == null) {
      //this fields combination not existed yet, add new dimension
      //dimensionsDescriptorIDToKeys don't keep the time bucket combination.
      if (dimensionsDescriptorIDToKeys.add(new Fields(allKeys))) {
        combinationId = dimensionsDescriptorIDToKeys.size() - 1;
      } else {
        throw new RuntimeException("The keys " + allKeys + " already existed.");
      }

      keysToCombinationId.put(allKeys, combinationId);
      addValueToAggregatorToCombination(values, allKeys, aggregatorName);
    } else {
      //if the combination existed, check the aggregator and add the aggregator if not added.
      Set<String> incrementalAggregatorNames;
      boolean isOTFAggregator = false;
      if (!isIncrementalAggregator(aggregatorName)) {
        //For OTF aggregator, need to and its depended incremental aggregators
        incrementalAggregatorNames = getOTFDependedIncrementalAggregatorNames(aggregatorName);
        isOTFAggregator = true;
      } else {
        incrementalAggregatorNames = Sets.newHashSet();
        incrementalAggregatorNames.add(aggregatorName);
      }

      Map<String, Set<String>> newValueToIncrementalAggregators = Maps.newHashMap();
      Map<String, Set<String>> newValueToOTFAggregators = Maps.newHashMap();
      for (String value : values) {
        newValueToIncrementalAggregators.put(value, incrementalAggregatorNames);
        if (isOTFAggregator) {
          newValueToOTFAggregators.put(value, Sets.newHashSet(aggregatorName));
        }
      }

      int ddid = combinationId * customTimeBucketsCombination.size();
      for (int index = 0; index < customTimeBucketsCombination.size(); ++index, ++ddid) {
        //for incremental aggregator, newValueToOTFAggregators is empty;
        //for OTF, both newValueToIncrementalAggregators and newValueToOTFAggregators should be merged.
        mergeMaps(dimensionsDescriptorIDToValueToAggregator.get(ddid), newValueToIncrementalAggregators);
        mergeMaps(dimensionsDescriptorIDToValueToOTFAggregator.get(ddid), newValueToOTFAggregators);
      }
    }
  }

  /**
   * @param values         the fields of value to be computed
   * @param allKeys        the allKeys represents an combination
   * @param aggregatorName the name of the aggregator(incremental or OTF) to be added to this combination
   */
  protected void addValueToAggregatorToCombination(Set<String> values, Set<String> allKeys, String aggregatorName)
  {
    Map<String, Set<String>> valueToIncrementalAggregators = Maps.newHashMap();
    Map<String, Set<String>> valueToOTFAggregators = Maps.newHashMap();

    Set<String> incrementalAggregatorNames;
    boolean isOTFAggregator = false;
    if (!isIncrementalAggregator(aggregatorName)) {
      //For OTF aggregator, need to and its depended incremental aggregators
      incrementalAggregatorNames = getOTFDependedIncrementalAggregatorNames(aggregatorName);
      isOTFAggregator = true;
    } else {
      incrementalAggregatorNames = Sets.newHashSet();
      incrementalAggregatorNames.add(aggregatorName);
    }
    for (String value : values) {
      valueToIncrementalAggregators.put(value, incrementalAggregatorNames);
      if (isOTFAggregator) {
        valueToOTFAggregators.put(value, Sets.newHashSet(aggregatorName));
      }
    }

    for (CustomTimeBucket customTimeBucket : customTimeBucketsCombination) {
      dimensionsDescriptorIDToValueToAggregator.add(valueToIncrementalAggregators);
      dimensionsDescriptorIDToValueToOTFAggregator.add(valueToOTFAggregators);

      //add empty information just for alignment
      dimensionsDescriptorIDToValueToCompositeAggregator.add(Collections.<String, Set<String>>emptyMap());
      dimensionsDescriptorIDToFieldToAggregatorAdditionalValues.add(Collections.<String, Set<String>>emptyMap());

      //key descriptor
      DimensionsDescriptor dimensionsDescriptor = new DimensionsDescriptor(customTimeBucket, new Fields(allKeys));
      dimensionsDescriptorIDToDimensionsDescriptor.add(dimensionsDescriptor);
      dimensionsDescriptorIDToKeyDescriptor.add(dimensionsDescriptor.createFieldsDescriptor(keyDescriptor));
    }
  }

  protected boolean isIncrementalAggregator(String aggregatorName)
  {
    return aggregatorRegistry.getNameToIncrementalAggregator().get(aggregatorName) != null;
  }

  protected boolean isOTFAggregator(String aggregatorName)
  {
    return aggregatorRegistry.getNameToOTFAggregators().get(aggregatorName) != null;
  }

  protected Set<String> getOTFDependedIncrementalAggregatorNames(String oftAggregatorName)
  {
    return Sets.newHashSet(
        aggregatorRegistry.getOTFAggregatorToIncrementalAggregators().get(oftAggregatorName).iterator());
  }

  /**
   * The value must be defined in the FIELD_VALUES section.
   * The additional section don't have value type information add can't define value.
   *
   * @param valueName
   * @param valueNames
   */
  protected void verifyValueDefined(String valueName, Set<String> valueNames)
  {
    if (valueNames == null || !valueNames.contains(valueName)) {
      throw new IllegalArgumentException("The additional value " + valueName + "Does not have a corresponding value "
          + valueName + " defined in the " + FIELD_VALUES + " section.");
    }
  }

  protected boolean isJsonSimpleString(String string)
  {
    return !string.contains("{") && !string.contains("[");
  }

  protected Object addNonCompositeAggregator(
      String aggregatorName,
      Map<String, Set<String>> allValueToAggregator,
      Map<String, Set<String>> allValueToOTFAggregator,
      String valueName,
      Set<String> aggregatorSet,
      Map<String, Type> aggregatorToType,
      Type typeT,
      Set<String> aggregatorOTFSet,
      boolean checkDuplicate
  )
  {
    if (aggregatorRegistry.isIncrementalAggregator(aggregatorName)) {
      Set<String> aggregatorNames = allValueToAggregator.get(valueName);

      if (aggregatorNames == null) {
        aggregatorNames = Sets.newHashSet();
        allValueToAggregator.put(valueName, aggregatorNames);
      }

      aggregatorNames.add(aggregatorName);

      if (!aggregatorSet.add(aggregatorName) && checkDuplicate) {
        throw new IllegalArgumentException("An aggregator " + aggregatorName
            + " cannot be specified twice for a value");
      }

      IncrementalAggregator aggregator = aggregatorRegistry.getNameToIncrementalAggregator().get(aggregatorName);
      aggregatorToType.put(aggregatorName, aggregator.getOutputType(typeT));
      return aggregator;

    }

    if (aggregatorRegistry.isOTFAggregator(aggregatorName)) {
      //Check for duplicate on the fly aggregators
      Set<String> aggregatorNames = allValueToOTFAggregator.get(valueName);

      if (aggregatorNames == null) {
        aggregatorNames = Sets.newHashSet();
        allValueToOTFAggregator.put(valueName, aggregatorNames);
      }

      if (!aggregatorNames.add(aggregatorName) && checkDuplicate) {
        throw new IllegalArgumentException("An aggregator " + aggregatorName +
            " cannot be specified twice for a value");
      }

      aggregatorOTFSet.add(aggregatorName);

      //Add child aggregators
      aggregatorNames = allValueToAggregator.get(valueName);

      if (aggregatorNames == null) {
        aggregatorNames = Sets.newHashSet();
        allValueToAggregator.put(valueName, aggregatorNames);
      }

      OTFAggregator aggregator = aggregatorRegistry.getNameToOTFAggregators().get(aggregatorName);
      aggregatorNames.addAll(aggregatorRegistry.getOTFAggregatorToIncrementalAggregators().get(aggregatorName));
      aggregatorSet.addAll(aggregatorRegistry.getOTFAggregatorToIncrementalAggregators().get(aggregatorName));
      aggregatorToType.put(aggregatorName, aggregator.getOutputType());
      LOG.debug("field name {} and adding aggregator names {}:", valueName, aggregatorNames);

      return aggregator;
    }

    throw new IllegalArgumentException(aggregatorName + " is not a valid non-composit aggregator.");
  }

  protected CompositeAggregator addCompositeAggregator(
      String aggregatorType,
      Map<String, Set<String>> allValueToCompositeAggregator,
      Set<String> aggregateCompositeSet,
      String valueName,
      String embededAggregatorName,
      Map<String, Object> properties,
      Map<String, Type> aggregatorToType
  )
  {
    if (!aggregatorRegistry.isTopBottomAggregatorType(aggregatorType)) {
      throw new IllegalArgumentException(aggregatorType + " is not a valid composite aggregator.");
    }

    final String aggregatorName = compositeAggregatorFactory.getCompositeAggregatorName(aggregatorType,
        embededAggregatorName, properties);
    final CompositeAggregator aggregator = compositeAggregatorFactory.createCompositeAggregator(aggregatorType,
        embededAggregatorName, properties);

    //Check for duplicate
    Set<String> aggregatorNames = allValueToCompositeAggregator.get(valueName);

    if (aggregatorNames == null) {
      aggregatorNames = Sets.newHashSet();
      allValueToCompositeAggregator.put(valueName, aggregatorNames);
    }

    if (!aggregatorNames.add(aggregatorName)) {
      throw new IllegalArgumentException("An aggregator " + aggregatorName +
          " cannot be specified twice for value '" + valueName + "'");
    }
    allValueToCompositeAggregator.put(valueName, aggregatorNames);

    aggregateCompositeSet.add(aggregatorName);

    //we don't know how to handle a generic composite aggregator, handle Top/Bottom aggregator here only
    //Add aggregator to the repository
    if (aggregator instanceof AbstractTopBottomAggregator) {
      aggregatorRegistry.getNameToTopBottomAggregator().put(aggregatorName, (AbstractTopBottomAggregator)aggregator);
    }

    LOG.debug("field name {} and adding aggregator names {}:", valueName, aggregatorNames);

    if (aggregatorToType != null) {
      aggregatorToType.put(aggregatorName, aggregator.getOutputType());
    }

    return aggregator;
  }

  /**
   * This is a helper method which retrieves the schema tags from the {@link JSONObject} if they are present.
   *
   * @param jo The {@link JSONObject} to retrieve schema tags from.
   * @return A list containing the retrieved schema tags. The list is empty if there are no schema tags present.
   */
  //TODO To be removed when Malhar Library 3.3 becomes a dependency.
  private List<String> getTags(JSONObject jo) throws JSONException
  {
    if (jo.has(FIELD_TAGS)) {
      return getStringsFromJSONArray(jo.getJSONArray(FIELD_TAGS));
    } else {
      return Lists.newArrayList();
    }
  }

  private Set<Set<String>> buildCombinations(Set<String> fields)
  {
    if (fields.isEmpty()) {
      Set<Set<String>> combinations = Sets.newHashSet();
      Set<String> combination = Sets.newHashSet();
      combinations.add(combination);
      return combinations;
    }

    fields = Sets.newHashSet(fields);
    String item = fields.iterator().next();
    fields.remove(item);

    Set<Set<String>> combinations = buildCombinations(fields);
    Set<Set<String>> newCombinations = Sets.newHashSet(combinations);

    for (Set<String> combination : combinations) {
      Set<String> newCombination = Sets.newHashSet(combination);
      newCombination.add(item);
      newCombinations.add(newCombination);
    }

    return newCombinations;
  }

  /**
   * Precondition: all depended aggregators( for example AVG depended on SUM and COUNT, Composite Aggregators
   * depended on embed aggregators )
   * should already solved. This function will not handle this dependencies.
   */
  protected void buildDimensionsDescriptorIDAggregatorIDMaps()
  {
    dimensionsDescriptorIDToIncrementalAggregatorIDs = Lists.newArrayList();
    dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor = Lists.newArrayList();
    dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor = Lists.newArrayList();

    for (int index = 0;
        index < dimensionsDescriptorIDToAggregatorToAggregateDescriptor.size();
        index++) {
      IntArrayList aggIDList = new IntArrayList();
      Int2ObjectMap<FieldsDescriptor> inputMap = new Int2ObjectOpenHashMap<>();
      Int2ObjectMap<FieldsDescriptor> outputMap = new Int2ObjectOpenHashMap<>();

      dimensionsDescriptorIDToIncrementalAggregatorIDs.add(aggIDList);
      dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor.add(inputMap);
      dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor.add(outputMap);

      for (Map.Entry<String, FieldsDescriptor> entry :
          dimensionsDescriptorIDToAggregatorToAggregateDescriptor.get(index).entrySet()) {
        buildNonCompositeAggregatorIDMap(entry.getKey(), entry.getValue(), aggIDList, inputMap, outputMap);
      }
    }

    //get the max aggregator id for generating the composite aggregator id
    int maxAggregatorID = getLargestNonCompositeAggregatorID();

    //assign aggregatorID to composite aggregators
    dimensionsDescriptorIDToCompositeAggregatorIDs = Lists.newArrayList();
    for (int index = 0;
        index < dimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor.size();
        index++) {
      IntArrayList aggIDList = new IntArrayList();
      //NOTE: share same map with incremental aggreator. As the input FD and output FD will be get from aggregatorID,
      // so it should be ok to share same map.
      Int2ObjectMap<FieldsDescriptor> inputMap = dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor.get(
          index);
      Int2ObjectMap<FieldsDescriptor> outputMap = dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor.get(
          index);

      dimensionsDescriptorIDToCompositeAggregatorIDs.add(aggIDList);

      for (Map.Entry<String, FieldsDescriptor> entry :
          dimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor.get(index).entrySet()) {
        String aggregatorName = entry.getKey();
        FieldsDescriptor inputDescriptor = entry.getValue();
        AbstractCompositeAggregator compositeAggregator = aggregatorRegistry.getNameToTopBottomAggregator().get(
            aggregatorName);

        //simple use ++ to assign aggregator id
        int aggregatorID;
        Integer objAggregatorID = aggregatorRegistry.getTopBottomAggregatorNameToID().get(aggregatorName);
        if (objAggregatorID == null) {
          aggregatorID = ++maxAggregatorID;
          aggregatorRegistry.getTopBottomAggregatorNameToID().put(aggregatorName, aggregatorID);
        } else {
          aggregatorID = objAggregatorID;
        }
        aggIDList.add(aggregatorID);
        inputMap.put(aggregatorID, inputDescriptor);
        //buildNonCompositeAggregatorIDMap(getEmbededAggregatorName(aggregatorName), entry.getValue(), aggIDList,
        // inputMap, outputMap);

        outputMap.put(aggregatorID, AggregatorUtils.getOutputFieldsDescriptor(inputDescriptor, compositeAggregator));
      }
    }
  }

  protected int getLargestNonCompositeAggregatorID()
  {
    int maxAggregatorID = 0;
    Collection<Integer> aggregatorIDs = aggregatorRegistry.getIncrementalAggregatorNameToID().values();
    for (int aggregatorID : aggregatorIDs) {
      if (aggregatorID > maxAggregatorID) {
        maxAggregatorID = aggregatorID;
      }
    }
    return maxAggregatorID;
  }

  protected void buildNonCompositeAggregatorIDMap(String aggregatorName, FieldsDescriptor inputDescriptor,
      IntArrayList aggIDList, Int2ObjectMap<FieldsDescriptor> inputMap, Int2ObjectMap<FieldsDescriptor> outputMap)
  {
    IncrementalAggregator incrementalAggregator = aggregatorRegistry.getNameToIncrementalAggregator().get(
        aggregatorName);
    //don't need to build OTF aggregate
    if (incrementalAggregator == null) {
      return;
    }
    int aggregatorID = aggregatorRegistry.getIncrementalAggregatorNameToID().get(aggregatorName);
    mergeAggregatorID(aggIDList, aggregatorID);
    inputMap.put(aggregatorID, inputDescriptor);
    outputMap.put(aggregatorID,
        AggregatorUtils.getOutputFieldsDescriptor(inputDescriptor,
        incrementalAggregator));
  }

  /**
   * fulfill the embed ddids of composite aggregators
   * get the dimensional descriptor of composite aggregator; genereate field keys of embed aggregator
   */
  protected void fulfillCompositeAggregatorExtraInfo()
  {
    Map<Set<String>, Integer> keysToCombinationId = getKeysToCombinationId();
    final int timeBucketSize = customTimeBuckets.size();

    for (int index = 0; index < dimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor.size(); ++index) {
      Map<String, FieldsDescriptor> compositeAggregatorNameToDescriptor =
          dimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor.get(index);
      for (String compositeAggregatorName : compositeAggregatorNameToDescriptor.keySet()) {
        AbstractTopBottomAggregator compositeAggregator = aggregatorRegistry.getNameToTopBottomAggregator().get(
            compositeAggregatorName);

        //set DimensionDescriptorID
        compositeAggregator.setDimensionDescriptorID(index);

        //aggregator id
        compositeAggregator.setAggregatorID(
            aggregatorRegistry.getTopBottomAggregatorNameToID().get(compositeAggregatorName));

        //keys for embed aggregator
        Set<String> keys = Sets.newHashSet();
        DimensionsDescriptor dd = dimensionsDescriptorIDToDimensionsDescriptor.get(index);

        keys.addAll(dd.getFields().getFieldsList());

        {
          Set<String> compositeKeys = Sets.newHashSet();
          compositeKeys.addAll(keys);
          compositeAggregator.setFields(compositeKeys);

          compositeAggregator.setAggregateDescriptor(compositeAggregatorNameToDescriptor.get(compositeAggregatorName));
        }

        keys.addAll(compositeAggregator.getSubCombinations());

        Integer combinationId = keysToCombinationId.get(keys);
        if (combinationId == null) {
          throw new RuntimeException("Can't find combination id for keys: " + keys);
        }
        for (int ddid = combinationId * timeBucketSize; ddid < (combinationId + 1) * timeBucketSize; ++ddid) {
          compositeAggregator.addEmbedAggregatorDdId(ddid);
        }
      }
    }
  }

  protected String getEmbededAggregatorName(String compositeAggregatorName)
  {
    try {
      return compositeAggregatorName.split("-")[1];
    } catch (Exception e) {
      throw new RuntimeException("Invalid Composite Aggregator Name: " + compositeAggregatorName);
    }
  }

  /**
   * add the aggregatorID into list if not existed
   *
   * @param aggIDList
   * @param aggregatorID
   */
  protected void mergeAggregatorID(IntArrayList aggIDList, int aggregatorID)
  {
    for (int index = 0; index < aggIDList.size(); ++index) {
      if (aggIDList.get(index) == aggregatorID) {
        return;
      }
    }
    aggIDList.add(aggregatorID);
  }

  private void mergeMaps(Map<String, Set<String>> destmap, Map<String, Set<String>> srcmap)
  {
    for (Map.Entry<String, Set<String>> entry : srcmap.entrySet()) {
      String key = entry.getKey();
      Set<String> destset = destmap.get(key);
      Set<String> srcset = srcmap.get(key);

      if (destset == null) {
        destset = Sets.newHashSet();
        destmap.put(key, destset);
      }

      if (srcset != null) {
        destset.addAll(srcset);
      }
    }
  }

  private FieldsDescriptor keyDescriptorWithTime(Map<String, Type> fieldToTypeWithTime,
      List<CustomTimeBucket> customTimeBuckets)
  {
    if (customTimeBuckets.size() > 1
        || (!customTimeBuckets.isEmpty() && !customTimeBuckets.get(0).getTimeBucket().equals(TimeBucket.ALL))) {
      fieldToTypeWithTime.put(DimensionsDescriptor.DIMENSION_TIME, DimensionsDescriptor.DIMENSION_TIME_TYPE);
    }

    return new FieldsDescriptor(fieldToTypeWithTime);
  }

  /**
   * Returns the {@link FieldsDescriptor} object for all key fields.
   *
   * @return The {@link FieldsDescriptor} object for all key fields.
   */
  public FieldsDescriptor getKeyDescriptor()
  {
    return keyDescriptor;
  }

  /**
   * Returns the {@link FieldsDescriptor} object for all aggregate values.
   *
   * @return The {@link FieldsDescriptor} object for all aggregate values.
   */
  public FieldsDescriptor getInputValuesDescriptor()
  {
    return inputValuesDescriptor;
  }

  /**
   * Returns the key {@link FieldsDescriptor} object corresponding to the given dimensions descriptor ID.
   *
   * @return The key {@link FieldsDescriptor} object corresponding to the given dimensions descriptor ID.
   */
  public List<FieldsDescriptor> getDimensionsDescriptorIDToKeyDescriptor()
  {
    return dimensionsDescriptorIDToKeyDescriptor;
  }

  /**
   * Returns a map from a {@link DimensionsDescriptor} to its corresponding id.
   *
   * @return A map from a {@link DimensionsDescriptor} to its corresponding id.
   */
  public Map<DimensionsDescriptor, Integer> getDimensionsDescriptorToID()
  {
    return dimensionsDescriptorToID;
  }

  /**
   * Returns the dimensionsDescriptorIDToDimensionsDescriptor.
   *
   * @return The dimensionsDescriptorIDToDimensionsDescriptor.
   */
  public List<DimensionsDescriptor> getDimensionsDescriptorIDToDimensionsDescriptor()
  {
    return dimensionsDescriptorIDToDimensionsDescriptor;
  }

  /**
   * Returns the dimensionsDescriptorIDToValueToAggregator.
   *
   * @return The dimensionsDescriptorIDToValueToAggregator.
   */
  public List<Map<String, Set<String>>> getDimensionsDescriptorIDToValueToAggregator()
  {
    return dimensionsDescriptorIDToValueToAggregator;
  }

  /**
   * Returns a JSON string which contains all the key information for this schema.
   *
   * @return A JSON string which contains all the key information for this schema.
   */
  public String getKeysString()
  {
    return keysString;
  }

  /**
   * Returns a JSON string which contains all the time bucket information for this schema.
   *
   * @return A JSON string which contains all the time bucket information for this schema.
   */
  public String getBucketsString()
  {
    return bucketsString;
  }

  /**
   * Returns a map from keys to the list of enums associated with each key.
   *
   * @return A map from keys to the list of enums associated with each key.
   */
  public Map<String, List<Object>> getKeysToEnumValuesList()
  {
    return keysToEnumValuesList;
  }

  /**
   * Returns the dimensionsDescriptorIDToValueToOTFAggregator map.
   *
   * @return The dimensionsDescriptorIDToValueToOTFAggregator map.
   */
  public List<Map<String, Set<String>>> getDimensionsDescriptorIDToValueToOTFAggregator()
  {
    return dimensionsDescriptorIDToValueToOTFAggregator;
  }

  /**
   * Returns the dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor map.
   *
   * @return The dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor map.
   */
  public List<Int2ObjectMap<FieldsDescriptor>> getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor()
  {
    return dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor;
  }

  /**
   * Returns the dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor map.
   *
   * @return The dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor map.
   */
  public List<Int2ObjectMap<FieldsDescriptor>> getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor()
  {
    return dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor;
  }

  /**
   * Returns the dimensionsDescriptorIDToAggregatorIDs map.
   *
   * @return The dimensionsDescriptorIDToAggregatorIDs map.
   */
  public List<IntArrayList> getDimensionsDescriptorIDToIncrementalAggregatorIDs()
  {
    return dimensionsDescriptorIDToIncrementalAggregatorIDs;
  }

  /**
   * this is the old interface, keep it as was.
   *
   * @return The dimensionsDescriptorIDToAggregatorIDs map.
   */
  public List<IntArrayList> getDimensionsDescriptorIDToAggregatorIDs()
  {
    return getDimensionsDescriptorIDToIncrementalAggregatorIDs();
  }

  public List<IntArrayList> getDimensionsDescriptorIDToCompositeAggregatorIDs()
  {
    return dimensionsDescriptorIDToCompositeAggregatorIDs;
  }


  /**
   * Returns the dimensionsDescriptorIDToKeys map.
   *
   * @return The dimensionsDescriptorIDToKeys map.
   */
  public List<Fields> getDimensionsDescriptorIDToKeys()
  {
    return dimensionsDescriptorIDToKeys;
  }

  /**
   * Returns the dimensionsDescriptorIDToFieldToAggregatorAdditionalValues map.
   *
   * @return The dimensionsDescriptorIDToFieldToAggregatorAdditionalValues map.
   */
  public List<Map<String, Set<String>>> getDimensionsDescriptorIDToFieldToAggregatorAdditionalValues()
  {
    return dimensionsDescriptorIDToFieldToAggregatorAdditionalValues;
  }

  /**
   * Returns the schemaAllValueToAggregatorToType map.
   *
   * @return The schemaAllValueToAggregatorToType map.
   */
  public Map<String, Map<String, Type>> getSchemaAllValueToAggregatorToType()
  {
    return schemaAllValueToAggregatorToType;
  }

  /**
   * Return the time buckets used in this schema.
   *
   * @return The timeBuckets used in this schema.
   * @deprecated use {@link #getCustomTimeBuckets()} instead.
   */
  @Deprecated
  public List<TimeBucket> getTimeBuckets()
  {
    return timeBuckets;
  }

  public List<CustomTimeBucket> getCustomTimeBuckets()
  {
    return customTimeBuckets;
  }

  public CustomTimeBucketRegistry getCustomTimeBucketRegistry()
  {
    return customTimeBucketRegistry;
  }

  /**
   * Gets the dimensionsDescriptorIDToAggregatorToAggregateDescriptor.
   *
   * @return The dimensionsDescriptorIDToAggregatorToAggregateDescriptor.
   */
  @VisibleForTesting
  public List<Map<String, FieldsDescriptor>> getDimensionsDescriptorIDToAggregatorToAggregateDescriptor()
  {
    return dimensionsDescriptorIDToAggregatorToAggregateDescriptor;
  }

  /**
   * Gets the dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor.
   *
   * @return The dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor.
   */
  @VisibleForTesting
  public List<Map<String, FieldsDescriptor>> getDimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor()
  {
    return dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor;
  }

  @VisibleForTesting
  public List<Map<String, FieldsDescriptor>> getDimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor()
  {
    return dimensionsDescriptorIDToCompositeAggregatorToAggregateDescriptor;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 97 * hash + (this.keyDescriptor != null ? this.keyDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.inputValuesDescriptor != null ? this.inputValuesDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.keysToEnumValuesList != null ? this.keysToEnumValuesList.hashCode() : 0);
    hash = 97 * hash +
        (this.dimensionsDescriptorIDToKeyDescriptor != null ? this.dimensionsDescriptorIDToKeyDescriptor.hashCode() :
            0);
    hash = 97 * hash + (this.dimensionsDescriptorIDToDimensionsDescriptor != null ?
        this.dimensionsDescriptorIDToDimensionsDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.dimensionsDescriptorIDToValueToAggregator != null ?
        this.dimensionsDescriptorIDToValueToAggregator.hashCode() : 0);
    hash = 97 * hash + (this.dimensionsDescriptorIDToValueToOTFAggregator != null ?
        this.dimensionsDescriptorIDToValueToOTFAggregator.hashCode() : 0);
    hash = 97 * hash + (this.dimensionsDescriptorIDToAggregatorToAggregateDescriptor != null ?
        this.dimensionsDescriptorIDToAggregatorToAggregateDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor != null ?
        this.dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.dimensionsDescriptorToID != null ? this.dimensionsDescriptorToID.hashCode() : 0);
    hash = 97 * hash + (this.dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor != null ?
        this.dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor.hashCode() : 0);
    hash = 97 * hash + (this.dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor != null ?
        this.dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor.hashCode() : 0);
    hash = 97 * hash +
        (this.dimensionsDescriptorIDToIncrementalAggregatorIDs != null ?
            this.dimensionsDescriptorIDToIncrementalAggregatorIDs.hashCode() :
            0);
    hash = 97 * hash + (this.dimensionsDescriptorIDToFieldToAggregatorAdditionalValues != null ?
        this.dimensionsDescriptorIDToFieldToAggregatorAdditionalValues.hashCode() : 0);
    hash = 97 * hash + (this.dimensionsDescriptorIDToKeys != null ? this.dimensionsDescriptorIDToKeys.hashCode() : 0);
    hash = 97 * hash + (this.keysString != null ? this.keysString.hashCode() : 0);
    hash = 97 * hash + (this.bucketsString != null ? this.bucketsString.hashCode() : 0);
    hash = 97 * hash + (this.aggregatorRegistry != null ? this.aggregatorRegistry.hashCode() : 0);
    hash = 97 * hash + (this.customTimeBuckets != null ? this.customTimeBuckets.hashCode() : 0);
    hash = 97 * hash +
        (this.schemaAllValueToAggregatorToType != null ? this.schemaAllValueToAggregatorToType.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final DimensionalConfigurationSchema other = (DimensionalConfigurationSchema)obj;
    if (this.keyDescriptor != other.keyDescriptor && (this.keyDescriptor == null || !this.keyDescriptor.equals(
        other.keyDescriptor))) {
      return false;
    }
    if (this.inputValuesDescriptor != other.inputValuesDescriptor &&
        (this.inputValuesDescriptor == null || !this.inputValuesDescriptor.equals(other.inputValuesDescriptor))) {
      return false;
    }
    if (this.keysToEnumValuesList != other.keysToEnumValuesList &&
        (this.keysToEnumValuesList == null || !this.keysToEnumValuesList.equals(other.keysToEnumValuesList))) {
      return false;
    }
    if (this.dimensionsDescriptorIDToKeyDescriptor != other.dimensionsDescriptorIDToKeyDescriptor &&
        (this.dimensionsDescriptorIDToKeyDescriptor == null ||
        !this.dimensionsDescriptorIDToKeyDescriptor.equals(other.dimensionsDescriptorIDToKeyDescriptor))) {
      return false;
    }
    if (this.dimensionsDescriptorIDToDimensionsDescriptor != other.dimensionsDescriptorIDToDimensionsDescriptor &&
        (this.dimensionsDescriptorIDToDimensionsDescriptor == null ||
        !this.dimensionsDescriptorIDToDimensionsDescriptor
        .equals(other.dimensionsDescriptorIDToDimensionsDescriptor))) {
      return false;
    }
    if (this.dimensionsDescriptorIDToValueToAggregator != other.dimensionsDescriptorIDToValueToAggregator &&
        (this.dimensionsDescriptorIDToValueToAggregator == null ||
        !this.dimensionsDescriptorIDToValueToAggregator.equals(other.dimensionsDescriptorIDToValueToAggregator))) {
      return false;
    }
    if (this.dimensionsDescriptorIDToValueToOTFAggregator != other.dimensionsDescriptorIDToValueToOTFAggregator &&
        (this.dimensionsDescriptorIDToValueToOTFAggregator == null ||
        !this.dimensionsDescriptorIDToValueToOTFAggregator
        .equals(other.dimensionsDescriptorIDToValueToOTFAggregator))) {
      return false;
    }
    if (this.dimensionsDescriptorIDToAggregatorToAggregateDescriptor !=
        other.dimensionsDescriptorIDToAggregatorToAggregateDescriptor &&
        (this.dimensionsDescriptorIDToAggregatorToAggregateDescriptor == null ||
        !this.dimensionsDescriptorIDToAggregatorToAggregateDescriptor.equals(
        other.dimensionsDescriptorIDToAggregatorToAggregateDescriptor))) {
      return false;
    }
    if (this.dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor !=
        other.dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor &&
        (this.dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor == null ||
        !this.dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor.equals(
        other.dimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor))) {
      return false;
    }
    if (this.dimensionsDescriptorToID != other.dimensionsDescriptorToID &&
        (this.dimensionsDescriptorToID == null || !this.dimensionsDescriptorToID.equals(
        other.dimensionsDescriptorToID))) {
      return false;
    }
    if (this.dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor !=
        other.dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor &&
        (this.dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor == null ||
        !this.dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor.equals(
        other.dimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor))) {
      return false;
    }
    if (this.dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor !=
        other.dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor &&
        (this.dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor == null ||
        !this.dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor.equals(
        other.dimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor))) {
      return false;
    }
    if (this.dimensionsDescriptorIDToIncrementalAggregatorIDs !=
        other.dimensionsDescriptorIDToIncrementalAggregatorIDs &&
        (this.dimensionsDescriptorIDToIncrementalAggregatorIDs == null ||
        !this.dimensionsDescriptorIDToIncrementalAggregatorIDs.equals(
        other.dimensionsDescriptorIDToIncrementalAggregatorIDs))) {
      return false;
    }
    if (this.dimensionsDescriptorIDToFieldToAggregatorAdditionalValues !=
        other.dimensionsDescriptorIDToFieldToAggregatorAdditionalValues &&
        (this.dimensionsDescriptorIDToFieldToAggregatorAdditionalValues == null ||
        !this.dimensionsDescriptorIDToFieldToAggregatorAdditionalValues.equals(
        other.dimensionsDescriptorIDToFieldToAggregatorAdditionalValues))) {
      return false;
    }
    if (this.dimensionsDescriptorIDToKeys != other.dimensionsDescriptorIDToKeys &&
        (this.dimensionsDescriptorIDToKeys == null || !this.dimensionsDescriptorIDToKeys.equals(
        other.dimensionsDescriptorIDToKeys))) {
      return false;
    }
    if ((this.keysString == null) ? (other.keysString != null) : !this.keysString.equals(other.keysString)) {
      return false;
    }
    if ((this.bucketsString == null) ? (other.bucketsString != null) : !this.bucketsString.equals(
        other.bucketsString)) {
      return false;
    }
    if (this.aggregatorRegistry != other.aggregatorRegistry &&
        (this.aggregatorRegistry == null || !this.aggregatorRegistry.equals(other.aggregatorRegistry))) {
      return false;
    }
    if (this.customTimeBuckets != other.customTimeBuckets &&
        (this.customTimeBuckets == null || !this.customTimeBuckets.equals(other.customTimeBuckets))) {
      return false;
    }
    return !(this.schemaAllValueToAggregatorToType != other.schemaAllValueToAggregatorToType &&
        (this.schemaAllValueToAggregatorToType == null || !this.schemaAllValueToAggregatorToType.equals(
            other.schemaAllValueToAggregatorToType)));
  }

  /**
   * @return the keyDescriptorWithTime
   */
  public FieldsDescriptor getKeyDescriptorWithTime()
  {
    return keyDescriptorWithTime;
  }

  /**
   * @param keyDescriptorWithTime the keyDescriptorWithTime to set
   */
  public void setKeyDescriptorWithTime(FieldsDescriptor keyDescriptorWithTime)
  {
    this.keyDescriptorWithTime = keyDescriptorWithTime;
  }

  /**
   * @return the keyToTags
   */
  public Map<String, List<String>> getKeyToTags()
  {
    return keyToTags;
  }

  /**
   * @return the valueToTags
   */
  public Map<String, List<String>> getValueToTags()
  {
    return valueToTags;
  }

  /**
   * @return the tags
   */
  public List<String> getTags()
  {
    return tags;
  }

  public Map<String, String> getKeyToExpression()
  {
    return keyToExpression;
  }

  public Map<String, String> getValueToExpression()
  {
    return valueToExpression;
  }

  /**
   * This class represents a value in the {@link DimensionalConfigurationSchema}.
   */
  public static class Value
  {
    /**
     * The name of the value.
     */
    private String name;
    /**
     * The type of the value.
     */
    private Type type;
    /**
     * The aggregations to be performed on this value accross all dimensions combinations.
     */
    private Set<String> aggregators;

    /**
     * This creates a value with the given name and type, which has the given aggregations
     * performed across all dimensionsCombinations.
     *
     * @param name        The name of the value.
     * @param type        The type of the value.
     * @param aggregators The aggregations performed across all dimensionsCombinations.
     */
    public Value(String name,
        Type type,
        Set<String> aggregators)
    {
      setName(name);
      setType(type);
      setAggregators(aggregators);
    }

    /**
     * This is a helper method which sets and validates the name of the value.
     *
     * @param name The name of the value.
     */
    private void setName(@NotNull String name)
    {
      this.name = Preconditions.checkNotNull(name);
    }

    /**
     * This is a helper method which sets and validated the type of the value.
     *
     * @param type The type of the value.
     */
    private void setType(@NotNull Type type)
    {
      this.type = Preconditions.checkNotNull(type);
    }

    /**
     * This is a helper method which sets and validates the aggregations performed
     * on the value across all dimensions combinations.
     *
     * @param aggregators The aggregations performed on the value across all dimensions combinations.
     */
    private void setAggregators(@NotNull Set<String> aggregators)
    {
      Preconditions.checkNotNull(aggregators);

      for (String aggregator : aggregators) {
        Preconditions.checkNotNull(aggregator);
      }

      this.aggregators = Sets.newHashSet(aggregators);
    }

    /**
     * Returns the name of the value.
     *
     * @return The name of the value.
     */
    public String getName()
    {
      return name;
    }

    /**
     * Returns the type of the value.
     *
     * @return The type of the value.
     */
    public Type getType()
    {
      return type;
    }

    /**
     * The aggregations performed on this value across all dimensions combinations.
     *
     * @return The aggregations performed on this value across all dimensions combinations.
     */
    public Set<String> getAggregators()
    {
      return aggregators;
    }
  }

  /**
   * This class represents a key in the {@link DimensionalConfigurationSchema}.
   */
  public static class Key
  {
    /**
     * The name of the key.
     */
    private String name;
    /**
     * The type of the key.
     */
    private Type type;
    /**
     * Any enum values associated with this key.
     */
    private List<Object> enumValues;

    /**
     * This creates a key definition for the {@link DimensionalConfigurationSchema}.
     *
     * @param name       The name of the key.
     * @param type       The type of the key.
     * @param enumValues Any enum values associated with the key.
     */
    public Key(String name,
        Type type,
        List<Object> enumValues)
    {
      setName(name);
      setType(type);
      setEnumValues(enumValues);
    }

    /**
     * This is a helper method to validate and set the name of the key.
     *
     * @param name The name of the key.
     */
    private void setName(@NotNull String name)
    {
      this.name = Preconditions.checkNotNull(name);
    }

    /**
     * This is a helper method to validate and set the type of the key.
     *
     * @param type The type of the key.
     */
    private void setType(@NotNull Type type)
    {
      this.type = Preconditions.checkNotNull(type);
    }

    /**
     * This is a helper method to set and validate the enum values for this key.
     *
     * @param enumValues The enum values for this key.
     */
    private void setEnumValues(@NotNull List<Object> enumValues)
    {
      Preconditions.checkNotNull(enumValues);

      for (Object values : enumValues) {
        Preconditions.checkNotNull(values);
      }

      this.enumValues = enumValues;
    }

    /**
     * Gets the name of this key.
     *
     * @return The name of this key.
     */
    public String getName()
    {
      return name;
    }

    /**
     * Gets the type of this key.
     *
     * @return The type of this key.
     */
    public Type getType()
    {
      return type;
    }

    /**
     * The enum values for this key.
     *
     * @return The enum values for this key.
     */
    public List<Object> getEnumValues()
    {
      return enumValues;
    }
  }

  /**
   * This class represents a dimensions combination in a {@link DimensionalConfigurationSchema}.
   */
  public static class DimensionsCombination
  {
    /**
     * The key fields in the dimensions combination.
     */
    private Fields fields;
    /**
     * A mapping from value name to the name of all the aggregations performed on the value. for
     * this dimensions combination.
     */
    private Map<String, Set<String>> valueToAggregators;

    /**
     * This creates a dimensions combination for {@link DimensionalConfigurationSchema}.
     *
     * @param fields             The key fields which this dimensions combination applies to.
     * @param valueToAggregators A mapping from value name to the name of all the aggregations
     *                           performed on the value.
     */
    public DimensionsCombination(Fields fields,
        Map<String, Set<String>> valueToAggregators)
    {
      setFields(fields);
      setValueToAggregators(valueToAggregators);
    }

    /**
     * This is a helper method which sets and validates the keys for this dimensions combination.
     *
     * @param fields The keys for this dimensions combination.
     */
    private void setFields(@NotNull Fields fields)
    {
      this.fields = Preconditions.checkNotNull(fields);
    }

    /**
     * Returns the key fields for this dimensions combination.
     *
     * @return The key fields for this dimensions combination.
     */
    public Fields getFields()
    {
      return fields;
    }

    /**
     * This is a helper method which sets and validates the given map from value to the set of
     * aggregations performed on that value.
     *
     * @param valueToAggregators The map from value to the set of aggregations performed on that value.
     */
    private void setValueToAggregators(@NotNull Map<String, Set<String>> valueToAggregators)
    {
      Preconditions.checkNotNull(valueToAggregators);
      Map<String, Set<String>> newValueToAggregators = Maps.newHashMap();

      for (Map.Entry<String, Set<String>> entry : valueToAggregators.entrySet()) {
        Preconditions.checkNotNull(entry.getKey());
        Preconditions.checkNotNull(entry.getValue());

        newValueToAggregators.put(entry.getKey(), Sets.newHashSet(entry.getValue()));

        for (String aggregator : entry.getValue()) {
          Preconditions.checkNotNull(aggregator);
        }
      }

      this.valueToAggregators = newValueToAggregators;
    }

    /**
     * Returns the map from value to the set of aggregations performed on that value.
     *
     * @return the map from value to the set of aggregations performed on that value.
     */
    public Map<String, Set<String>> getValueToAggregators()
    {
      return valueToAggregators;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionalConfigurationSchema.class);
}
