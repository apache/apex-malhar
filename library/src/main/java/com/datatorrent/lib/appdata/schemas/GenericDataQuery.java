/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.Query;
import com.google.common.base.Preconditions;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericDataQuery extends Query
{
  public static final String FIELD_DATA = "data";
  public static final String FIELD_TIME = "time";
  public static final String FIELD_FROM = "from";
  public static final String FIELD_TO = "to";
  public static final String FIELD_LATEST_NUM_BUCKETS = "latestNumBuckets";
  public static final String FIELD_BUCKET = "bucket";

  public static final String FIELD_KEYS = "keys";
  public static final String FIELD_FIELDS = "fields";
  public static final String FIELD_COUNTDOWN = "countdown";
  public static final String FIELD_INCOMPLETE_RESULT_OK = "incompleteResultOK";

  private String from;
  private String to;
  private TimeBucket timeBucket;
  private GPOMutable keys;
  private List<String> fields;
  private long countdown;
  private boolean incompleteResultOK = true;

  public GenericDataQuery(String from,
                          String to,
                          Integer latestNumBuckets,
                          TimeBucket timeBucket,
                          GPOImmutable keys,
                          List<String> fields,
                          long countdown,
                          boolean incompleteResultOK)
  {
    setFrom(from);
    setTo(to);
    setTimeBucket(timeBucket);
    setKeys(keys);
    setFields(fields);
    setCountdown(countdown);
    setIncompleteResultOK(incompleteResultOK);
  }

  private void setIncompleteResultOK(boolean incompleteResultOK)
  {
    this.incompleteResultOK = incompleteResultOK;
  }

  public boolean getIncompleteResultOK()
  {
    return incompleteResultOK;
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

  private void setFrom(String from)
  {
    SchemaUtils.checkDateEx(from);
    this.from = from;
  }

  public String getFrom()
  {
    return from;
  }

  private void setTo(String to)
  {
    SchemaUtils.checkDateEx(to);
    this.to = to;
  }

  public String getTo()
  {
    return to;
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

  private void setKeys(GPOMutable keys)
  {
    Preconditions.checkNotNull(keys);
  }

  public GPOMutable getKeys()
  {
    return keys;
  }

  /**
   * @param fields the fields to set
   */
  private void setFields(List<String> fields)
  {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  /**
   * @return the fields
   */
  public List<String> getFields()
  {
    return fields;
  }
}
