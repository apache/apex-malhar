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
@DataSerializerInfo(clazz=GenericDataResultTabularSerializer.class)
public class GenericDataResultTabular extends Result
{
  public static final String TYPE = "dataResult";
  public static final String FIELD_COUNTDOWN = "countdown";

  private List<GPOMutable> values;
  private long countdown;
  private boolean oneTime;

  public GenericDataResultTabular()
  {
  }

  public GenericDataResultTabular(GenericDataQueryTabular query,
                                  List<GPOMutable> values)
  {
    super(query);

    setValues(values);

    this.oneTime = true;
  }

  public GenericDataResultTabular(GenericDataQueryTabular query,
                                  List<GPOMutable> values,
                                  long countdown)
  {
    super(query);

    setValues(values);
    setCountdown(countdown);

    this.oneTime = false;
  }

  private void setValues(List<GPOMutable> values)
  {
    Preconditions.checkNotNull(values);
    this.values = values;
  }

  @Override
  public GenericDataQueryTabular getQuery()
  {
    return (GenericDataQueryTabular) super.getQuery();
  }

  public List<GPOMutable> getValues()
  {
    return values;
  }

  private void setCountdown(long countdown)
  {
    this.countdown = countdown;
  }

  public long getCountdown()
  {
    return countdown;
  }

  /**
   * @return the oneTime
   */
  public boolean isOneTime()
  {
    return oneTime;
  }
}
