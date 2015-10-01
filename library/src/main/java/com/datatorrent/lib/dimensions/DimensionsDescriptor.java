/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions;

import java.io.Serializable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.schemas.CustomTimeBucket;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.appdata.schemas.Type;

/**
 * <p>
 * This class defines a dimensions combination which is used by dimensions computation operators
 * and stores. A dimension combination is composed of the names of the fields that constitute the key,
 * as well as the TimeBucket under which data is stored.
 * </p>
 * <p>
 * This class supports the creation of a dimensions combination from a {@link TimeBucket} object and a set of fields.
 * It also supports the creation of a dimensions combination an aggregation string. An aggregation string looks like
 * the following:
 * <br/>
 * <br/>
 * {@code
 *  "time=MINUTES:publisher:advertiser"
 * }
 * <br/>
 * <br/>
 * In the example above <b>"time=MINUTES"</b> represents a time bucket, and the other colon separated strings represent
 * the name of fields which comprise the key for this dimension combination. When specifiying a time bucket in an
 * aggregation string you must use the name of one of the TimeUnit enums.
 * </p>
 * <p>
 * One of the primary uses of a {@link DimensionsDescriptor} is for querying a dimensional data store. When a query is
 * received for a dimensional data store, the query must be mapped to many things including a dimensionDescriptorID. The
 * dimensionDescriptorID is an id assigned to a class of dimension combinations which share the same keys. This mapping is
 * performed by creating a {@link DimensionsDescriptor} object from the query, and then using the {@link DimensionsDescriptor} object
 * to look up the correct dimensionsDescriptorID. This lookup to retrieve a dimensionsDescriptorID is necessary because a
 * dimensionsDescriptorID is used for storage in order to prevent key conflicts.
 * </p>
 * @since 3.1.0
 *
 */
public class DimensionsDescriptor implements Serializable, Comparable<DimensionsDescriptor>
{
  private static final long serialVersionUID = 201506251237L;

  /**
   * Name of the reserved time field.
   */
  public static final String DIMENSION_TIME = "time";
  /**
   * Type of the reserved time field.
   */
  public static final Type DIMENSION_TIME_TYPE = Type.LONG;
  /**
   * Name of the reserved time bucket field.
   */
  public static final String DIMENSION_TIME_BUCKET = "timeBucket";
  /**
   * Type of the reserved time bucket field.
   */
  public static final Type DIMENSION_TIME_BUCKET_TYPE = Type.INTEGER;
  /**
   * The set of fields used for time, which are intended to be queried. Not that the
   * timeBucket field is not included here because its not intended to be queried.
   */
  public static final Fields TIME_FIELDS = new Fields(Sets.newHashSet(DIMENSION_TIME));
  /**
   * This set represents the field names which cannot be part of the user defined field names in a schema for
   * dimensions computation.
   */
  public static final Set<String> RESERVED_DIMENSION_NAMES = ImmutableSet.of(DIMENSION_TIME,
                                                                             DIMENSION_TIME_BUCKET);
  /**
   * This is the equals string separator used when defining a time bucket for a dimensions combination.
   */
  public static final String DELIMETER_EQUALS = "=";
  /**
   * This separates dimensions in the dimensions combination.
   */
  public static final String DELIMETER_SEPERATOR = ":";
  /**
   * A map from a key field to its type.
   */
  public static final Map<String, Type> DIMENSION_FIELD_TO_TYPE;

  /**
   * The time bucket used for this dimension combination.
   */
  private TimeBucket timeBucket;
  /**
   * The custom time bucket used for this dimension combination.
   */
  private CustomTimeBucket customTimeBucket;
  /**
   * The set of key fields which compose this dimension combination.
   */
  private Fields fields;

  static
  {
    Map<String, Type> dimensionFieldToType = Maps.newHashMap();

    dimensionFieldToType.put(DIMENSION_TIME, DIMENSION_TIME_TYPE);
    dimensionFieldToType.put(DIMENSION_TIME_BUCKET, DIMENSION_TIME_BUCKET_TYPE);

    DIMENSION_FIELD_TO_TYPE = Collections.unmodifiableMap(dimensionFieldToType);
  }

  /**
   * Constructor for kryo serialization.
   */
  private DimensionsDescriptor()
  {
    //for kryo
  }

  /**
   * Creates a dimensions descriptor (dimensions combination) with the given {@link TimeBucket} and key fields.
   * @param timeBucket The {@link TimeBucket} that this dimensions combination represents.
   * @param fields The key fields included in this dimensions combination.
   *
   * @deprecated use {@link #DimensionsDescriptor(com.datatorrent.lib.appdata.schemas.CustomTimeBucket, com.datatorrent.lib.appdata.schemas.Fields)} instead.
   */
  @Deprecated
  public DimensionsDescriptor(TimeBucket timeBucket,
                              Fields fields)
  {
    setTimeBucket(timeBucket);
    setFields(fields);
  }

  /**
   * Creates a dimensions descriptor (dimensions combination) with the given {@link CustomTimeBucket} and key fields.
   *
   * @param timeBucket The {@link CustomTimeBucket} that this dimensions combination represents.
   * @param fields The key fields included in this dimensions combination.
   */
  public DimensionsDescriptor(CustomTimeBucket timeBucket,
                              Fields fields)
  {
    setCustomTimeBucket(timeBucket);
    setFields(fields);
  }

  /**
   * Creates a dimensions descriptor (dimensions combination) with the given key fields.
   * @param fields The key fields included in this dimensions combination.
   */
  public DimensionsDescriptor(Fields fields)
  {
    setFields(fields);
  }

  /**
   * This construction creates a dimensions descriptor (dimensions combination) from the given aggregation string.
   * @param aggregationString The aggregation string to use when initializing this dimensions combination.
   */
  public DimensionsDescriptor(String aggregationString)
  {
    initialize(aggregationString);
  }

  /**
   * Initializes the dimensions combination with the given aggregation string.
   * @param aggregationString The aggregation string with which to initialize this dimensions combination.
   */
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

  /**
   * This is a helper method which sets and validates the {@link TimeBucket}.
   * @param timeBucket The {@link TimeBucket} to set and validate.
   */
  private void setTimeBucket(TimeBucket timeBucket)
  {
    Preconditions.checkNotNull(timeBucket);
    this.timeBucket = timeBucket;
    this.customTimeBucket = new CustomTimeBucket(timeBucket);
  }

  /**
   * This is a helper method which sets and validates the {@link CustomTimeBucket}.
   * @param customTimeBucket The {@link CustomTimeBucket} to set and validate.
   */
  private void setCustomTimeBucket(CustomTimeBucket customTimeBucket)
  {
    Preconditions.checkNotNull(customTimeBucket);
    this.customTimeBucket = customTimeBucket;
    this.timeBucket = customTimeBucket.getTimeBucket();
  }

  /**
   * Gets the {@link TimeBucket} for this {@link DimensionsDescriptor} object.
   * @return The {@link TimeBucket} for this {@link DimensionsDescriptor} object.
   *
   * @deprecated use {@link #getCustomTimeBucket()} instead.
   */
  @Deprecated
  public TimeBucket getTimeBucket()
  {
    return timeBucket;
  }

  /**
   * Gets the {@link CustomTimeBucket} for this {@link DimensionsDescriptor} object.
   * @return The {@link CustomTimeBucket} for this {@link DimensionsDescriptor} object.
   */
  public CustomTimeBucket getCustomTimeBucket()
  {
    return customTimeBucket;
  }

  /**
   * This is a helper method which sets and validates the set of key fields for this
   * {@link DimensionsDescriptor} object.
   * @param fields The set of key fields for this {@link DimensionsDescriptor} object.
   */
  private void setFields(Fields fields)
  {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  /**
   * Returns the set of key fields for this {@link DimensionsDescriptor} object.
   * @return The set of key fields for this {@link DimensionsDescriptor} object.
   */
  public Fields getFields()
  {
    return fields;
  }

  /**
   * This method is used to create a new {@link FieldsDescriptor} object representing this
   * {@link DimensionsDescriptor} object from another {@link FieldsDescriptor} object which
   * defines the names and types of all the available key fields.
   * @param parentDescriptor The {@link FieldsDescriptor} object which defines the name and
   * type of all the available key fields.
   * @return A {@link FieldsDescriptor} object which represents this {@link DimensionsDescriptor} (dimensions combination)
   * derived from the given {@link FieldsDescriptor} object.
   */
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

    if(timeBucket != null && timeBucket != TimeBucket.ALL) {
      fieldToType.put(DIMENSION_TIME_BUCKET, DIMENSION_TIME_BUCKET_TYPE);
      fieldToType.put(DIMENSION_TIME, Type.LONG);
    }

    return new FieldsDescriptor(fieldToType);
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 83 * hash + (this.customTimeBucket != null ? this.customTimeBucket.hashCode() : 0);
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
    if(!this.customTimeBucket.equals(other.customTimeBucket)) {
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
    return "DimensionsDescriptor{" + "timeBucket=" + customTimeBucket + ", fields=" + fields + '}';
  }

  @Override
  public int compareTo(DimensionsDescriptor other)
  {
    if (this == other) {
      return 0;
    }

    List<String> thisFieldList = this.getFields().getFieldsList();
    List<String> otherFieldList = other.getFields().getFieldsList();

    if (thisFieldList != otherFieldList) {
      int compare = thisFieldList.size() - otherFieldList.size();

      if (compare != 0) {
        return compare;
      }

      Collections.sort(thisFieldList);
      Collections.sort(otherFieldList);

      for (int index = 0; index < thisFieldList.size(); index++) {
        String thisField = thisFieldList.get(index);
        String otherField = otherFieldList.get(index);

        int fieldCompare = thisField.compareTo(otherField);

        if (fieldCompare != 0) {
          return fieldCompare;
        }
      }
    }

    CustomTimeBucket thisBucket = this.getCustomTimeBucket();
    CustomTimeBucket otherBucket = other.getCustomTimeBucket();

    if (thisBucket == null && otherBucket == null) {
      return 0;
    } else if (thisBucket != null && otherBucket == null) {
      return 1;
    } else if (thisBucket == null && otherBucket != null) {
      return -1;
    } else {
      return thisBucket.compareTo(otherBucket);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsDescriptor.class);
}
