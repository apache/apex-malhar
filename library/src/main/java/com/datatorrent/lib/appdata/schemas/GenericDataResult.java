/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.Result;
import com.google.common.base.Preconditions;

import java.util.List;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericDataResult extends Result
{
  public static final String FIELD_COUNTDOWN = "countdown";

  private List<GPOMutable> values;
  private long countdown;
  private GenericDataQuery dataQuery;

  public GenericDataResult(GenericDataQuery dataQuery,
                           List<GPOMutable> values,
                           long countdown)
  {
    super(dataQuery);
    setValues(values);
    setCountdown(countdown);
    setDataQuery(dataQuery);
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

  /**
   * @return the countdown
   */
  public long getCountdown()
  {
    return countdown;
  }
}
