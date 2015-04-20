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

import com.datatorrent.common.util.DTThrowable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {"timeBuckets":["1m","1h","1d"],
 *  "values":
 *   [{"name":"valueName1","type":"type1","aggregators":["SUM"]},
 *    {"name":"valueName2","type":"type2","aggregators":["SUM"]}]
 * }
 */
public class AggregationEventSchema
{
  public static final String FIELD_TIMEBUCKETS = "timeBuckets";
  public static final String FIELD_VALUES = "values";

  public static final String FIELD_VALUES_NAME = "name";
  public static final String FIELD_VALUES_TYPE = "type";
  public static final String FIELD_VALUES_AGGREGATIONS = "aggregators";

  public static final List<Fields> VALID_KEY_FIELDS = ImmutableList.of(new Fields(Sets.newHashSet(FIELD_TIMEBUCKETS,
                                                                                                  FIELD_VALUES)));
  private Set<TimeBucket> timeBuckets;
  private FieldsDescriptor inputValuesDescriptor;
  private Map<String, Set<String>> allValueToAggregator;

  public AggregationEventSchema(String aggregationJSON)
  {
    try {
      initialize(aggregationJSON);
    }
    catch(Exception e) {
      DTThrowable.rethrow(e);
    }
  }

  private void initialize(String aggregationJSON) throws Exception
  {
    JSONObject json = new JSONObject(aggregationJSON);

    SchemaUtils.checkValidKeys(json, VALID_KEY_FIELDS);

    //Time buckets

    JSONArray timeBucketsArray = json.getJSONArray(FIELD_TIMEBUCKETS);
    timeBuckets = Sets.newHashSet();

    for(int index = 0;
        index < timeBucketsArray.length();
        index++) {
      TimeBucket timeBucket = TimeBucket.getBucketEx(timeBucketsArray.getString(index));

      if(!timeBuckets.add(timeBucket)) {
        throw new IllegalArgumentException("The time bucket " + timeBucket + " was listed multiple times.");
      }
    }

    timeBuckets = Collections.unmodifiableSet(timeBuckets);

    //Value To Aggregator

    Map<String, Type> fieldToType = Maps.newHashMap();
    Map<String, Set<String>> tempAllValueToAggregator = Maps.newHashMap();

    JSONArray valuesArray = json.getJSONArray(FIELD_VALUES);

    for(int valueIndex = 0;
        valueIndex < valuesArray.length();
        valueIndex++) {
      JSONObject value = valuesArray.getJSONObject(valueIndex);
      String valueName = value.getString(FIELD_VALUES_NAME);
      String valueType = value.getString(FIELD_VALUES_TYPE);

      fieldToType.put(valueName, Type.getTypeEx(valueType));

      Set<String> aggregators = Sets.newHashSet();
      JSONArray aggregatorsArray = value.getJSONArray(FIELD_VALUES_AGGREGATIONS);

      for(int aggregatorIndex = 0;
          aggregatorIndex < aggregatorsArray.length();
          aggregatorIndex++) {
        aggregators.add(aggregatorsArray.getString(aggregatorIndex));
      }

      tempAllValueToAggregator.put(valueName, aggregators);
    }

    inputValuesDescriptor = new FieldsDescriptor(fieldToType);

    allValueToAggregator = Maps.newHashMap();

    for(Map.Entry<String, Set<String>> entry: tempAllValueToAggregator.entrySet()) {
      String valueName = entry.getKey();
      Set<String> aggregators = entry.getValue();

      allValueToAggregator.put(valueName, Collections.unmodifiableSet(aggregators));
    }

    allValueToAggregator = Collections.unmodifiableMap(allValueToAggregator);
  }

  public FieldsDescriptor getInputValuesDescriptor()
  {
    return inputValuesDescriptor;
  }

  public Set<TimeBucket> getTimeBuckets()
  {
    return timeBuckets;
  }

  public Map<String, Set<String>> getAllValueToAggregator()
  {
    return allValueToAggregator;
  }
}
