/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class DimensionsDescriptor
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsDescriptor.class);

  public static final String DIMENSION_TIME = "time";
  public static final Type DIMENSION_TIME_TYPE = Type.LONG;
  public static final String DIMENSION_TIME_BUCKET = "timeBucket";
  public static final Type DIMENSION_TIME_BUCKET_TYPE = Type.INTEGER;

  public static final Set<String> RESERVED_DIMENSION_NAMES = ImmutableSet.of(DIMENSION_TIME,
                                                                             DIMENSION_TIME_BUCKET);

  public static final String DELIMETER_EQUALS = "=";
  public static final String DELIMETER_SEPERATOR = ":";

  private TimeBucket timeBucket;
  private Fields fields;
  private String aggregationString;

  public DimensionsDescriptor(String aggregationString)
  {
    setAggregationString(aggregationString);
    initialize();
  }

  private void initialize()
  {
    String[] fieldArray = aggregationString.split(DELIMETER_SEPERATOR);
    Set<String> fieldSet = Sets.newHashSet();

    for(String field: fieldArray) {
      String[] fieldAndValue = field.split(DELIMETER_EQUALS);
      String fieldName = fieldAndValue[0];
      fieldSet.add(fieldName);

      if(fieldName.equals(DIMENSION_TIME)) {
        if(fieldAndValue.length == 2) {
          fieldSet.add(DIMENSION_TIME_BUCKET);
          timeBucket = TimeBucket.TIME_UNIT_TO_TIME_BUCKET.get(TimeUnit.valueOf(fieldAndValue[1]));
        }
      }
    }

    fields = new Fields(fieldSet);
  }

  private void setAggregationString(String aggregationString)
  {
    Preconditions.checkNotNull(aggregationString);
    this.aggregationString = aggregationString;
  }

  public TimeBucket getTimeBucket()
  {
    return timeBucket;
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

    if(fields.getFields().contains(DIMENSION_TIME)) {
      fieldToType.put(DIMENSION_TIME, Type.LONG);
    }

    if(timeBucket != null) {
      fieldToType.put(DIMENSION_TIME_BUCKET, DIMENSION_TIME_BUCKET_TYPE);
    }

    return new FieldsDescriptor(fieldToType);
  }

  @Override
  public int hashCode()
  {
    int hash = 3;
    hash = 53 * hash + (this.aggregationString != null ? this.aggregationString.hashCode() : 0);
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
    if((this.aggregationString == null) ? (other.aggregationString != null) : !this.aggregationString.equals(other.aggregationString)) {
      return false;
    }
    return true;
  }

  public static boolean validateDimensions(DimensionsSchema schema,
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

      if(!schema.getKeyFieldDescriptor().getFields().getFields().contains(fieldName)) {
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
}
