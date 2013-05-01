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
import java.util.*;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class DimensionTimeBucketOperator extends BaseOperator
{
  @InputPortFieldAnnotation(name = "in", optional = false)
  public final transient DefaultInputPort<Map<String, Object>> in = new DefaultInputPort<Map<String, Object>>(this)
  {
    @Override
    public void process(Map<String, Object> tuple)
    {
      long time = extractTimeFromTuple(tuple);
      calendar.setTimeInMillis(time);
      List<String> timeBucketList = new ArrayList<String>();

      if ((timeBucketFlags | TIMEBUCKET_YEAR) != 0) {
        timeBucketList.add(String.format("YEAR|%04d", calendar.get(Calendar.YEAR)));
      }
      if ((timeBucketFlags | TIMEBUCKET_MONTH) != 0) {
        timeBucketList.add(String.format("MONTH|%04d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH)));
      }
      if ((timeBucketFlags | TIMEBUCKET_WEEK) != 0) {
        timeBucketList.add(String.format("WEEK|%04d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.WEEK_OF_YEAR)));
      }
      if ((timeBucketFlags | TIMEBUCKET_DAY) != 0) {
        timeBucketList.add(String.format("DAY|%04d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH)));
      }
      if ((timeBucketFlags | TIMEBUCKET_HOUR) != 0) {
        timeBucketList.add(String.format("HOUR|%04d%02d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY)));
      }
      if ((timeBucketFlags | TIMEBUCKET_MINUTE) != 0) {
        timeBucketList.add(String.format("MINUTE|%04d%02d%02d%02d%02d", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE)));
      }


      for (String timeBucket : timeBucketList) {
        for (List<String> dimensionCombination : dimensionCombinations) {
          String key = new String();
          Number value = (Number)tuple.get(valueKeyName);
          for (String dimension : dimensionCombination) {
            if (!key.isEmpty()) {
              key += "|";
            }
            key += dimension + ":" + tuple.get(dimension).toString();
          }
          process(timeBucket, key, value);
        }
      }
    }

  };
  @OutputPortFieldAnnotation(name = "out", optional = false)
  public final transient DefaultOutputPort<Map<String, Map<String, Number>>> out = new DefaultOutputPort<Map<String, Map<String, Number>>>(this);
  private List<String> dimensionKeyNames = new ArrayList<String>();
  private String valueKeyName;
  private String timeKeyName;
  private long currentWindowId = 0;
  private long windowWidth;
  private int timeBucketFlags;
  private TimeZone timeZone = TimeZone.getTimeZone("GMT");
  private Calendar calendar = new GregorianCalendar(timeZone);
  private List<List<String>> dimensionCombinations = new ArrayList<List<String>>();
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

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowWidth = context.getApplicationAttributes().attrValue(DAGContext.STRAM_WINDOW_SIZE_MILLIS, null);

    for (int i = 1; i <= dimensionKeyNames.size(); i++) {
      dimensionCombinations.addAll(Combinations.getCombinations(dimensionKeyNames, i));
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

  public void setValueKeyName(String key)
  {
    valueKeyName = key;
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

  public abstract void process(String timeBucket, String key, Number value);

}
