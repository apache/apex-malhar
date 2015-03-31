/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.DataDeserializerInfo;
import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.DataValidatorInfo;
import com.datatorrent.lib.appdata.qr.Query;
import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */

@DataType(type=GenericDataQueryTabular.TYPE)
@DataDeserializerInfo(clazz=GenericDataQueryTabularDeserializer.class)
@DataValidatorInfo(clazz=GenericDataQueryTabularValidator.class)
public class GenericDataQueryTabular extends Query
{
  public static final String TYPE = "dataQuery";

  public static final String FIELD_DATA = "data";
  public static final String FIELD_FIELDS = "fields";
  public static final String FIELD_COUNTDOWN = "countdown";

  private boolean oneTime = false;
  private Fields fields;
  private long countdown;

  public GenericDataQueryTabular()
  {
  }

  public GenericDataQueryTabular(String id,
                                 String type,
                                 Fields fields)
  {
    super(id,
          type);

    setFields(fields);
    this.oneTime = true;
  }

  public GenericDataQueryTabular(String id,
                                 String type,
                                 Fields fields,
                                 long countdown)
  {
    super(id,
          type);

    setFields(fields);
    setCountdown(countdown);
    this.oneTime = false;
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

  /**
   * @return the oneTime
   */
  public boolean isOneTime()
  {
    return oneTime;
  }

  /**
   * @param oneTime the oneTime to set
   */
  public void setOneTime(boolean oneTime)
  {
    this.oneTime = oneTime;
  }

  private void setCountdown(long countdown)
  {
    Preconditions.checkArgument(countdown > 0, "Countdown must be positive.");
    this.countdown = countdown;
  }

  public long getCountdown()
  {
    return countdown;
  }
}
