/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * <p>
 * Abstract DimensionTimeBucketOperator class.
 * </p>
 *
 * @since 0.3.2
 */
public abstract class DimensionTimeBucketOperator extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(DimensionTimeBucketOperator.class);
  @InputPortFieldAnnotation(name = "in", optional = false)
  public final transient DefaultInputPort<Map<String, Object>> in = new DefaultInputPort<Map<String, Object>>() {
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

        // System.out.println(dimensionCombinations.size()+ " testing");
        for (String timeBucket : timeBucketList) {
          for (int[] dimensionCombination : dimensionCombinations) {
            String field = "0";
            String key = new String();
            if (dimensionCombination != null) {
              for (int d : dimensionCombination) {
                if (!key.isEmpty()) {
                  key += "|";
                }
                key += String.valueOf(d) + ":" + tuple.get(dimensionKeyNames.get(d)).toString();
              }
            }
            DimensionTimeBucketOperator.this.process(timeBucket, key, field, 1);
            for (int i = 0; i < valueKeyNames.size(); i++) {
              String valueKeyName = valueKeyNames.get(i);
              field = String.valueOf(i + 1);
              Object value = tuple.get(valueKeyName);
              Number numberValue = extractNumber(valueKeyName, value);
              DimensionTimeBucketOperator.this.process(timeBucket, key, field, numberValue);
            }
          }
        }
      } catch (Exception ex) {
        LOG.warn("Got exception for tuple {}.  Ignoring this tuple.", tuple, ex);
      }
    }

  };
  /**
   * First String key is the bucket Second String key is the key Third String
   * key is the field
   */
  @OutputPortFieldAnnotation(name = "out", optional = false)
  public final transient DefaultOutputPort<Map<String, Map<String, Number>>> out = new DefaultOutputPort<Map<String, Map<String, Number>>>();
  private List<String> dimensionKeyNames = new ArrayList<String>();
  private List<String> valueKeyNames = new ArrayList<String>();
  private String timeKeyName;
  private long currentWindowId;
  private long windowWidth =500;
  private int timeBucketFlags;
  private transient TimeZone timeZone = TimeZone.getTimeZone("GMT");
  private transient Calendar calendar = new GregorianCalendar(timeZone);
  private transient List<int[]> dimensionCombinations = new ArrayList<int[]>();
  private List<Set<String>> dimensionCombinationsSet;
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
    } else {
      time = (Long) tuple.get(timeKeyName);
    }
    return time;
  }

  protected Number extractNumber(String valueKeyName, Object value)
  {
    if (value instanceof Number) {
      return (Number) value;
    } else if (value == null) {
      return new Long(0);
    } else {
      try {
        return numberFormat.parse(value.toString());
      } catch (ParseException ex) {
      }
    }
    return new Long(0);
  }

  /*
   * When calling this, the operator no longer expands all combinations of all
   * keys but instead only use the combinations the caller supplies
   * 
   * @param keys The key combination to add
   */
  public void addCombination(Set<String> keys) throws NoSuchFieldException
  {
    if (dimensionCombinationsSet == null) {
      dimensionCombinationsSet = new ArrayList<Set<String>>();
    }
    dimensionCombinationsSet.add(keys);
    // if (keys == null) {
    // dimensionCombinations.add(null);
    // }
    // else {
    // // slow but this function doesn't get executed many times and
    // dimensionKeyNames is small.
    // int indexKeys[] = new int[keys.size()];
    // int i = 0;
    // for (String key : keys) {
    // indexKeys[i] = -1;
    // for (int j = 0; j < dimensionKeyNames.size(); j++) {
    // if (dimensionKeyNames.get(j).equals(key)) {
    // indexKeys[i] = j;
    // break;
    // }
    // }
    // if (indexKeys[i] < 0) {
    // throw new NoSuchFieldException("Key not found: " + key);
    // }
    // i++;
    // }
    // dimensionCombinations.add(indexKeys);
    // }
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if(context != null)
      windowWidth = context.getValue(DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
    if (dimensionCombinations.isEmpty() && dimensionCombinationsSet == null) {
      dimensionCombinations.add(null);
      for (int i = 1; i <= dimensionKeyNames.size(); i++) {
        dimensionCombinations.addAll(Combinations.getNumberCombinations(dimensionKeyNames.size(), i));
      }
    } else if (dimensionCombinationsSet != null) {
      for (Set<String> keySet : dimensionCombinationsSet) {
        int indexKeys[] = new int[keySet.size()];
        int i = 0;
        for (String key : keySet) {
          indexKeys[i] = -1;
          for (int j = 0; j < dimensionKeyNames.size(); j++) {
            if (dimensionKeyNames.get(j).equals(key)) {
              indexKeys[i] = j;
              break;
            }
          }
          if (indexKeys[i] < 0) {

          }
          i++;
        }
        dimensionCombinations.add(indexKeys);
      }
    }
    logger.info("number of combinations {}",dimensionCombinations.size());
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

  public static class Combinations
  {
    public static <K> List<List<K>> getCombinations(List<K> list, int r)
    {
      List<List<K>> result = new ArrayList<List<K>>();
      List<int[]> combos = getNumberCombinations(list.size(), r);

      for (int[] a : combos) {
        List<K> keys = new ArrayList<K>(r);
        for (int j = 0; j < r; j++) {
          keys.add(j, list.get(a[j]));
        }
        result.add(keys);
      }

      return result;
    }

    public static List<int[]> getNumberCombinations(int n, int r)
    {
      List<int[]> list = new ArrayList<int[]>();
      int[] res = new int[r];
      for (int i = 0; i < res.length; i++) {
        res[i] = i;
      }
      boolean done = false;
      while (!done) {
        int[] a = new int[res.length];
        System.arraycopy(res, 0, a, 0, res.length);
        list.add(a);
        done = next(res, n, r);
      }
      return list;
    }

    private static boolean next(int[] num, int n, int r)
    {
      int target = r - 1;
      num[target]++;
      if (num[target] > ((n - (r - target)))) {
        // Carry the One
        while (num[target] > ((n - (r - target) - 1))) {
          target--;
          if (target < 0) {
            break;
          }
        }
        if (target < 0) {
          return true;
        }
        num[target]++;
        for (int i = target + 1; i < num.length; i++) {
          num[i] = num[i - 1] + 1;
        }
      }
      return false;
    }

    public static void main(String[] args)
    {
      String[] list = new String[] { "a", "b", "c", "d", "e" };
      for (int i = 1; i <= list.length; i++) {
        logger.info("Combinations: {}", getCombinations(Arrays.asList(list), i));
      }
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(DimensionTimeBucketOperator.class);
}
