/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAGContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class DimensionTimeBucketOperator extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(DimensionTimeBucketOperator.class);
  @InputPortFieldAnnotation(name = "in", optional = false)
  public final transient DefaultInputPort<Map<String, Object>> in = new DefaultInputPort<Map<String, Object>>(this)
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      try {
        long time = extractTimeFromTuple(tuple);
        calendar.setTimeInMillis(time);
        List<String> timeBucketList = new ArrayList<String>();

        if ((timeBucketFlags & TIMEBUCKET_YEAR) != 0) {
          timeBucketList.add(String.format("Y|%04d", calendar.get(Calendar.YEAR)));
        }
        if ((timeBucketFlags & TIMEBUCKET_MONTH) != 0) {
          timeBucketList.add(String.format("M|%04d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1));
        }
        if ((timeBucketFlags & TIMEBUCKET_WEEK) != 0) {
          timeBucketList.add(String.format("W|%04d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.WEEK_OF_YEAR)));
        }
        if ((timeBucketFlags & TIMEBUCKET_DAY) != 0) {
          timeBucketList.add(String.format("D|%04d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH)));
        }
        if ((timeBucketFlags & TIMEBUCKET_HOUR) != 0) {
          timeBucketList.add(String.format("h|%04d%02d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY)));
        }
        if ((timeBucketFlags & TIMEBUCKET_MINUTE) != 0) {
          timeBucketList.add(String.format("m|%04d%02d%02d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE)));
        }


        for (String timeBucket: timeBucketList) {
          for (int[] dimensionCombination: dimensionCombinations) {
            String field = "0";
            String key = new String();
            for (int d: dimensionCombination) {
              if (!key.isEmpty()) {
                key += "|";
              }
              key += String.valueOf(d) + ":" + tuple.get(dimensionKeyNames.get(d)).toString();
            }
            DimensionTimeBucketOperator.this.process(timeBucket, key, field, 1);
            for (int i = 0; i < valueKeyNames.size(); i++) {
              String valueKeyName = valueKeyNames.get(i);
              field = String.valueOf(i + 1);
              Object value = tuple.get(valueKeyName);
              Number numberValue = extractNumber(valueKeyName, value);
              key = "";
              for (int d: dimensionCombination) {
                if (!key.isEmpty()) {
                  key += "|";
                }
                key += String.valueOf(d) + ":" + tuple.get(dimensionKeyNames.get(d)).toString();
              }
              DimensionTimeBucketOperator.this.process(timeBucket, key, field, numberValue);
            }
          }
        }
      }
      catch (Exception ex) {
        LOG.warn("Got exception for tuple {}.  Ignoring this tuple.", tuple, ex);
      }
    }

  };
  /**
   * First String key is the bucket
   * Second String key is the key
   * Third String key is the field
   */
  @OutputPortFieldAnnotation(name = "out", optional = false)
  public final transient DefaultOutputPort<Map<String, Map<String, Number>>> out = new DefaultOutputPort<Map<String, Map<String, Number>>>(this);
  private List<String> dimensionKeyNames = new ArrayList<String>();
  private List<String> valueKeyNames = new ArrayList<String>();
  private String timeKeyName;
  private long currentWindowId;
  private long windowWidth;
  private int timeBucketFlags;
  private transient TimeZone timeZone = TimeZone.getTimeZone("GMT");
  private transient Calendar calendar = new GregorianCalendar(timeZone);
  private transient List<int[]> dimensionCombinations = new ArrayList<int[]>();
  private transient NumberFormat numberFormat = NumberFormat.getInstance();
  public static final int TIMEBUCKET_MINUTE = 1;
  public static final int TIMEBUCKET_HOUR = 2;
  public static final int TIMEBUCKET_DAY = 4;
  public static final int TIMEBUCKET_WEEK = 8;
  public static final int TIMEBUCKET_MONTH = 16;
  public static final int TIMEBUCKET_YEAR = 32;

  protected long extractTimeFromTuple(Map<String, Object> tuple)
  {
    long time;
    if (timeKeyName == null) {
      time = (currentWindowId >>> 32) * 1000 + windowWidth * (currentWindowId & 0xffffffffL);
    }
    else {
      time = (Long)tuple.get(timeKeyName);
    }
    return time;
  }

  protected Number extractNumber(String valueKeyName, Object value)
  {
    Number numberValue;
    if (value instanceof Number) {
      numberValue = (Number)value;
    }
    else {
      try {
        numberValue = numberFormat.parse(value.toString());
      }
      catch (ParseException ex) {
        return null;
      }
    }
    return numberValue;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowWidth = context.getApplicationAttributes().attrValue(DAGContext.STRAM_WINDOW_SIZE_MILLIS, null);

    for (int i = 1; i <= dimensionKeyNames.size(); i++) {
      dimensionCombinations.addAll(Combinations.getNumberCombinations(dimensionKeyNames.size(), i));
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    currentWindowId = windowId;
  }

  public void addDimensionKeyName(String key)
  {
    dimensionKeyNames.add(key);
  }

  public void addValueKeyName(String key)
  {
    valueKeyNames.add(key);
  }

  public void setTimeKeyName(String key)
  {
    timeKeyName = key;
  }

  public void setTimeBucketFlags(int timeBucketFlags)
  {
    this.timeBucketFlags = timeBucketFlags;
  }

  public void setTimeZone(TimeZone tz)
  {
    timeZone = tz;
    calendar.setTimeZone(timeZone);
  }

  public abstract void process(String timeBucket, String key, String field, Number value);

}
