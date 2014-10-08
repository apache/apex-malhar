/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.lib.statistics.DimensionsComputation;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes schema for performing dimensional computation on a stream of Map<String,Object> tuples.
 *
 * Schema can be specified as a JSON string with following keys.
 *
 *   fields: Map of all the field names and their types.  Supported types: java.lang.(Integer, Long, Float, Double, String)
 *   dimension: Array of dimensions with fields separated by colon, and time prefixed with time=.  Supported time units: MINUTES, HOURS, DAYS
 *   aggregates: Fields to aggregate for specified dimensions.  Aggregates types can include: sum, avg, min, max
 *   timestamp: Name of the timestamp field.  Data type should be Long with value in milliseconds since Jan 1, 1970 GMT.
 *
 * Example JSON schema for Ads demo:
 *
 *   {
 *     "fields": {"publisherId":"java.lang.Integer", "advertiserId":"java.lang.Integer", "adUnit":"java.lang.Integer", "clicks":"java.lang.Long", "price":"java.lang.Long", "cost":"java.lang.Double", "revenue":"java.lang.Double", "timestamp":"java.lang.Long"},
 *     "dimensions": ["time=MINUTES", "time=MINUTES:adUnit", "time=MINUTES:advertiserId", "time=MINUTES:publisherId", "time=MINUTES:advertiserId:adUnit", "time=MINUTES:publisherId:adUnit", "time=MINUTES:publisherId:advertiserId", "time=MINUTES:publisherId:advertiserId:adUnit"],
 *     "aggregates": { "clicks": "sum", "price": "sum", "cost": "sum", "revenue": "sum"},
 *     "timestamp": "timestamp"
 *   }
 *
 *
 *
 */
public class EventSchema implements Serializable
{
  private static final long serialVersionUID = 4586481500190519858L;

  /* Map of all the fields and their types in an event tuple */
  public Map<String, Class<?>> fields = Maps.newHashMap();

  /* Names of the fields which make up the keys */
  public List<String> keys = Lists.newArrayList();

  // Fields to aggregate mapped to aggregate operations and data types
  public Map<String, String> aggregates = Maps.newHashMap();

  // List of dimensional combinations to compute
  public List<String> dimensions = Lists.newArrayList();

  public String timestamp = "timestamp";

  // Used to map between event schema and generic event arrays for keys and values
  transient public List<String> genericEventKeys = Lists.newArrayList();
  transient public List<String> genericEventValues = Lists.newArrayList();

  transient private int keyLen;
  transient private int valLen;

  public static final String DEFAULT_SCHEMA_ADS = "{\n" +
          "  \"fields\": {\"publisherId\":\"java.lang.Integer\", \"advertiserId\":\"java.lang.Integer\", \"adUnit\":\"java.lang.Integer\", \"clicks\":\"java.lang.Long\", \"impressions\":\"java.lang.Long\", \"price\":\"java.lang.Long\", \"cost\":\"java.lang.Double\", \"revenue\":\"java.lang.Double\", \"timestamp\":\"java.lang.Long\"},\n" +
          "  \"dimensions\": [\"time=MINUTES\", \"time=MINUTES:adUnit\", \"time=MINUTES:advertiserId\", \"time=MINUTES:publisherId\", \"time=MINUTES:advertiserId:adUnit\", \"time=MINUTES:publisherId:adUnit\", \"time=MINUTES:publisherId:advertiserId\", \"time=MINUTES:publisherId:advertiserId:adUnit\"],\n" +
          "  \"aggregates\": { \"clicks\": \"sum\", \"price\": \"sum\", \"cost\": \"sum\", \"revenue\": \"sum\"},\n" +
          "  \"timestamp\": \"timestamp\"\n" +
          "}";
  public static final String DEFAULT_SCHEMA_SALES = "{\n" +
          "  \"fields\": {\"productId\":\"java.lang.Integer\",\"customerId\":\"java.lang.Integer\",\"channelId\":\"java.lang.Integer\",\"productCategory\":\"java.lang.String\",\"amount\":\"java.lang.Double\",\"timestamp\":\"java.lang.Long\"},\n" +
          "  \"dimensions\": [\"time=MINUTES\", \"time=MINUTES:productCategory\",\"time=MINUTES:channelId\",\"time=MINUTES:productCategory:channelId\"],\n" +
          "  \"aggregates\": { \"amount\": \"sum\" },\n" +
          "  \"timestamp\": \"timestamp\"\n" +
          "}";

  public static EventSchema createFromJSON(String json) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    EventSchema eventSchema = mapper.readValue(json, EventSchema.class);

    if ( eventSchema.dimensions.size() == 0 ) throw new IllegalArgumentException("EventSchema JSON must specify dimensions list");

    // Generate list of keys from dimensions specified
    Set<String> uniqueKeys = Sets.newHashSet();
    for(String dimension: eventSchema.dimensions) {
      String[] attributes = dimension.split(":");
      for (String attribute : attributes) {
        String[] keyval = attribute.split("=", 2);
        String key = keyval[0];
        if (key.equals("time")) {
          continue;
        }
        uniqueKeys.add(key);
      }
    }
    eventSchema.genericEventKeys.addAll(uniqueKeys);
    eventSchema.keys.addAll(uniqueKeys);
    eventSchema.keys.add(eventSchema.getTimestamp());

    eventSchema.genericEventValues.addAll(eventSchema.aggregates.keySet());

    return eventSchema;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setFields(Map<String, Class<?>> fields)
  {
    this.fields = fields;
  }

  public void setKeys(List<String> keys)
  {
    this.keys = keys;
  }

  public Collection<String> getGenericEventValues() {
    return aggregates.keySet();
  }

  public void setAggregates(Map<String, String> aggregates)
  {
    this.aggregates = aggregates;
  }

  public Class<?> getClass(String field) {
    return fields.get(field);
  }

  public int getKeyLen() {
    if (keyLen == 0)
      keyLen = getSerializedLength(keys);
    return keyLen;
  }

  public int getValLen() {
    if (valLen == 0)
      valLen = getSerializedLength(getGenericEventValues());
    return valLen;
  }

  public int getSerializedLength(Collection<String> fields) {
    int len = 0;
    for(String field : fields) {
      Class<?> k = this.fields.get(field);
      len += GenericAggregateSerializer.fieldSerializers.get(k).dataLength();
    }
    return len;
  }

  public Class<?> getType(String param)
  {
    return fields.get(param);
  }

  public Object typeCast(String input, String fieldKey) {
    if (input == null) return null;
    Class<?> c = getType(fieldKey);

    if (c.equals(Integer.class)) return Integer.valueOf(input);
    else if (c.equals(Long.class)) return Long.valueOf(input);
    else if (c.equals(Float.class)) return Float.valueOf(input);
    else if (c.equals(Double.class)) return Float.valueOf(input);
    else return input;
  }

  public static Map<Class,Object> DEFAULT_VALUES = Maps.newHashMap();
  static {
    DEFAULT_VALUES.put(Integer.class, 0);
    DEFAULT_VALUES.put(Long.class, 0L);
    DEFAULT_VALUES.put(Float.class, 0f);
    DEFAULT_VALUES.put(Double.class, 0d);
    DEFAULT_VALUES.put(String.class, "");
  }
  public Object defaultValue(String key) {
    Class<?> c = getType(key);
    return DEFAULT_VALUES.get(c);
  }


  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
  }


  Object getValue(GenericAggregate ga, String name) {
    if ( genericEventKeys.contains(name))
      return ga.keys[genericEventKeys.indexOf(name)];
    else if ( genericEventValues.contains(name))
      return ga.aggregates[genericEventValues.indexOf(name)];
    else
      return null;
  }

  public void defaultMissingFields(GenericAggregate ga) {
    for(int i=0; i < genericEventKeys.size(); i++)
    {
      if (ga.keys[i] == null)
        ga.keys[i] = defaultValue(genericEventKeys.get(i));
    }
    for(int i=0; i < genericEventValues.size(); i++)
    {
      if (ga.aggregates[i] == null)
        ga.aggregates[i] = defaultValue(genericEventValues.get(i));
    }
  }

  GenericEvent convertQueryKeysToGenericEvent(Map<String, String> keys)
  {
    GenericEvent event = new GenericEvent();
    event.keys = new Object[genericEventKeys.size()];
    int idx = 0;
    for(String key : genericEventKeys)
    {
      Object value = typeCast(keys.get(key), key);
      event.keys[idx++] = (value == null) ? defaultValue(key) : value;
    }

    event.values = new Object[genericEventValues.size()];
    idx = 0;
    for(String key : genericEventValues)
    {
      event.values[idx++] = defaultValue(key);
    }

    return event;
  }


  GenericEvent convertMapToGenericEvent(Map<String, Object> tuple)
  {
    GenericEvent event = new GenericEvent();
    Object timeValue = tuple.get(getTimestamp());
    if (timeValue != null) {
      event.timestamp = ((Number)timeValue).longValue();
    }
    event.keys = new Object[genericEventKeys.size()];
    int idx = 0;
    for(String key : genericEventKeys)
    {
      Object value = tuple.get(key);
      event.keys[idx++] = (value == null) ? defaultValue(key) : value;
    }
    event.values = new Object[genericEventValues.size()];
    idx = 0;
    for(String key : genericEventValues)
    {
      Object value = tuple.get(key);
      event.values[idx++] = (value == null) ? defaultValue(key) : value;
    }
    return event;
  }

  Map<String, Object> convertAggregateEventToMap(GenericAggregate ga) {
    Map<String, Object> map = Maps.newHashMap();

    for (int i=0; i < genericEventKeys.size(); i++) {
      map.put(genericEventKeys.get(i), ga.keys[i]);
    }

    for (int i=0; i < genericEventValues.size(); i++) {
      map.put(genericEventValues.get(i), ga.aggregates[i]);
    }

    map.put(timestamp, ga.getTimestamp());

    return map;
  }


  public Class getAggregateType(int i)
  {
    return getType(genericEventValues.get(i));
  }
}
