/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.schemas;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.serde.DataResultDimensionalSerializer;
import com.datatorrent.lib.appdata.query.serde.MessageSerializerInfo;
import com.datatorrent.lib.appdata.query.serde.MessageType;

/**
 * This class represents the result sent in response to a {@link DataQueryDimensional}
 *
 * @since 3.1.0
 */
@MessageType(type = DataResultDimensional.TYPE)
@MessageSerializerInfo(clazz = DataResultDimensionalSerializer.class)
public class DataResultDimensional extends Result
{
  /**
   * The AppData type of the result.
   */
  public static final String TYPE = "dataResult";

  /**
   * List of keys corresponding to each result. Each result is a map from aggregator
   * name to the key for the data result.
   */
  private List<Map<String, GPOMutable>> keys;
  /**
   * List of results. Each result is a map from aggregator name to the data values.
   */
  private List<Map<String, GPOMutable>> values;

  /**
   * This constructor creates a {@link DataResultDimensional} object with the
   * given data query, keys, and values.
   *
   * @param dataQuery The query that this result is a response to.
   * @param keys      A list of keys for the queried data. The n'th index in this list
   *                  corresponds to the n'th index in the values list.
   * @param values    A list of values for the queried data. The n'th index in this list
   *                  corresponds to the n'th index in the keys list.
   */
  public DataResultDimensional(DataQueryDimensional dataQuery,
      List<Map<String, GPOMutable>> keys,
      List<Map<String, GPOMutable>> values)
  {
    super(dataQuery);
    setKeys(keys);
    setValues(values);

    initialize();
  }

  /**
   * This constructor creates a {@link DataResultDimensional} object with the
   * given data query, keys, and values.
   *
   * @param dataQuery The data query that this result is a response to.
   * @param keys      A list of keys for the queried data. The n'th index in this list
   *                  corresponds to the n'th index in the values list.
   * @param values    A list of values for the queried data. The n'th index in this list
   *                  corresponds to the n'th index in the keys list.
   * @param countdown The countdown value for this result.
   */
  public DataResultDimensional(DataQueryDimensional dataQuery,
      List<Map<String, GPOMutable>> keys,
      List<Map<String, GPOMutable>> values,
      long countdown)
  {
    super(dataQuery,
        countdown);
    setKeys(keys);
    setValues(values);

    initialize();
  }

  /**
   * This is a helper method which performs validation functions required by
   * all the constructors.
   */
  private void initialize()
  {
    if (keys.size() != values.size()) {
      throw new IllegalArgumentException("The keys " + keys.size()
          + " and values " + values.size()
          + " arrays must be the same size.");
    }
  }

  @Override
  public DataQueryDimensional getQuery()
  {
    return (DataQueryDimensional)super.getQuery();
  }

  /**
   * This is a helper method which sets and validates the key values.
   *
   * @param keys The key values to set on the data result.
   */
  private void setKeys(List<Map<String, GPOMutable>> keys)
  {
    this.keys = Preconditions.checkNotNull(keys, "keys");
  }

  /**
   * Gets the key values for this result.
   *
   * @return The key values for this result.
   */
  public List<Map<String, GPOMutable>> getKeys()
  {
    return keys;
  }

  /**
   * Sets the values for this result.
   *
   * @param values The values for this result.
   */
  private void setValues(List<Map<String, GPOMutable>> values)
  {
    this.values = Preconditions.checkNotNull(values, "values");
  }

  /**
   * Gets the values for this result.
   *
   * @return The values for this result.
   */
  public List<Map<String, GPOMutable>> getValues()
  {
    return values;
  }
}
