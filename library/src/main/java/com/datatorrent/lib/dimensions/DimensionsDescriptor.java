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

import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DimensionsDescriptor
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsDescriptor.class);

  public static final Map<String, Type> DIMENSION_FIELD_TO_TYPE;

  public static final String DIMENSION_TIME = "time";
  public static final Type DIMENSION_TIME_TYPE = Type.LONG;

  public static final String DIMENSION_TIME_BUCKET = "timeBucket";
  public static final Type DIMENSION_TIME_BUCKET_TYPE = Type.INTEGER;

  public static final Fields TIME_FIELDS = new Fields(Sets.newHashSet(DIMENSION_TIME));
  public static final Set<String> RESERVED_DIMENSION_NAMES = ImmutableSet.of(DIMENSION_TIME,
                                                                             DIMENSION_TIME_BUCKET);

  public static final String DELIMETER_EQUALS = "=";
  public static final String DELIMETER_SEPERATOR = ":";

  private TimeBucket timeBucket;
  private Fields fields;

  static
  {
    Map<String, Type> dimensionFieldToType = Maps.newHashMap();

    dimensionFieldToType.put(DIMENSION_TIME, DIMENSION_TIME_TYPE);
    dimensionFieldToType.put(DIMENSION_TIME_BUCKET, DIMENSION_TIME_BUCKET_TYPE);

    DIMENSION_FIELD_TO_TYPE = Collections.unmodifiableMap(dimensionFieldToType);
  }

  private DimensionsDescriptor()
  {
    //for kryo
  }

  public DimensionsDescriptor(TimeBucket timeBucket,
                              Fields fields)
  {
    setTimeBucket(timeBucket);
    setFields(fields);
  }

  public DimensionsDescriptor(String aggregationString)
  {
    initialize(aggregationString);
  }

  public DimensionsDescriptor(TimeBucket timeBucket,
                              String aggregationString)
  {
    setTimeBucket(timeBucket);
    initialize(aggregationString);
  }

  private void initialize(String aggregationString)
  {
    String[] fieldArray = aggregationString.split(DELIMETER_SEPERATOR);
    Set<String> fieldSet = Sets.newHashSet();

    for(String field: fieldArray) {
      String[] fieldAndValue = field.split(DELIMETER_EQUALS);
      String fieldName = fieldAndValue[0];

      if(fieldName.equals(DIMENSION_TIME_BUCKET)) {
        throw new IllegalArgumentException(DIMENSION_TIME_BUCKET + " is an invalid time.");
      }

      if(!fieldName.equals(DIMENSION_TIME)) {
        fieldSet.add(fieldName);
      }

      if(fieldName.equals(DIMENSION_TIME)) {
        if(timeBucket != null) {
          throw new IllegalArgumentException("Cannot specify time in a dimensions "
                                             + "descriptor when a timebucket is also "
                                             + "specified.");
        }

        if(fieldAndValue.length == 2) {
          timeBucket = TimeBucket.TIME_UNIT_TO_TIME_BUCKET.get(TimeUnit.valueOf(fieldAndValue[1]));
        }
      }
    }

    fields = new Fields(fieldSet);
  }

  private void setTimeBucket(TimeBucket timeBucket)
  {
    Preconditions.checkNotNull(timeBucket);
    this.timeBucket = timeBucket;
  }

  public TimeBucket getTimeBucket()
  {
    return timeBucket;
  }

  private void setFields(Fields fields)
  {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  public Fields getFields()
  {
    return fields;
  }

  public FieldsDescriptor createFieldsDescriptor(FieldsDescriptor parentDescriptor)
  {
    Map<String, Type> fieldToType = Maps.newHashMap();
    Map<String, Type> parentFieldToType = parentDescriptor.getFieldToType();

    for(String field: this.fields.getFields()) {
      if(RESERVED_DIMENSION_NAMES.contains(field)) {
        continue;
      }

      fieldToType.put(field, parentFieldToType.get(field));
    }

    if(timeBucket != null) {
      fieldToType.put(DIMENSION_TIME_BUCKET, DIMENSION_TIME_BUCKET_TYPE);
      fieldToType.put(DIMENSION_TIME, Type.LONG);
    }

    return new FieldsDescriptor(fieldToType);
  }

  public static boolean validateDimensions(DimensionalSchema schema,
                                           String aggregationString)
  {
    if(aggregationString.isEmpty()) {
      logger.error("The dimensions string cannot be empty.");
      return false;
    }

    Set<String> fieldSet = Sets.newHashSet();
    String[] fields = aggregationString.split(DELIMETER_SEPERATOR);

    for(String field: fields) {
      String[] fieldAndValue = field.split(DELIMETER_EQUALS);

      if(fieldAndValue.length == 0) {
        logger.error("There is no field data in: {}", aggregationString);
        return false;
      }

      String fieldName = fieldAndValue[0];

      if(!schema.getGenericEventSchema().getAllKeysDescriptor().getFields().getFields().contains(fieldName)) {
        logger.error("{} is not a valid field.", fieldName);
        return false;
      }

      if(!fieldSet.add(fieldName)) {
        logger.error("Cannot duplicate the field {} in the aggregation string.", fieldName);
        return false;
      }

      if(fieldAndValue.length > 2) {
        logger.error("Cannot have more than one " +
                     DELIMETER_EQUALS +
                     " symbols in the field expression: {}",
                     field);
        return false;
      }
      else if(fieldAndValue.length == 2)
      {
        if(fieldName.equals(DIMENSION_TIME)) {
          String fieldValue = fieldAndValue[1];

          try {
            TimeUnit.valueOf(fieldValue);
          }
          catch(Exception e) {
            logger.error("No valid timeunit for " + fieldValue, e);
          }
        }
        else {
          logger.error("A bucket cannot be specified for field: {}", fieldName);
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 83 * hash + (this.timeBucket != null ? this.timeBucket.hashCode() : 0);
    hash = 83 * hash + (this.fields != null ? this.fields.hashCode() : 0);
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
    final DimensionsDescriptor other = (DimensionsDescriptor)obj;
    if(this.timeBucket != other.timeBucket) {
      return false;
    }
    if(this.fields != other.fields && (this.fields == null || !this.fields.equals(other.fields))) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "DimensionsDescriptor{" + "timeBucket=" + timeBucket + ", fields=" + fields + '}';
  }
}
