/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.DataSerializerInfo;
import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.Result;
import com.google.common.base.Preconditions;

import java.util.List;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */

@DataType(type=GenericDataQuery.TYPE)
@DataSerializerInfo(clazz=GenericDataResultSerializer.class)
public class GenericDataResult extends Result
{
  public static final String TYPE = "dataResult";
  public static final String FIELD_COUNTDOWN = "countdown";

  private List<GPOMutable> keys;
  private List<GPOMutable> values;
  private long countdown;
  private GenericDataQuery dataQuery;

  public GenericDataResult(GenericDataQuery dataQuery,
                           List<GPOMutable> keys,
                           List<GPOMutable> values,
                           long countdown)
  {
    super(dataQuery);
    setKeys(keys);
    setValues(values);
    setCountdown(countdown);
    setDataQuery(dataQuery);

    if(keys.size() != values.size()) {
      throw new IllegalArgumentException("The keys " + keys.size() +
                                         " and values " + values.size() +
                                         " arrays must be the same size.");
    }
  }

  private void setDataQuery(GenericDataQuery dataQuery)
  {
    Preconditions.checkNotNull(dataQuery);
    this.dataQuery = dataQuery;
  }

  public GenericDataQuery getDataQuery()
  {
    return dataQuery;
  }

  private void setKeys(List<GPOMutable> keys)
  {
    Preconditions.checkNotNull(keys);
    this.keys = keys;
  }

  private void setValues(List<GPOMutable> values)
  {
    Preconditions.checkNotNull(values);
    this.values = values;
  }

  private void setCountdown(long countdown)
  {
    this.countdown = countdown;
  }

  /**
   * @return the values
   */
  public List<GPOMutable> getValues()
  {
    return values;
  }

  public List<GPOMutable> getKeys()
  {
    return keys;
  }

  /**
   * @return the countdown
   */
  public long getCountdown()
  {
    return countdown;
  }
}
