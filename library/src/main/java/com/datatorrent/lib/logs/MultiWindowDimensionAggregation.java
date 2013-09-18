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
package com.datatorrent.lib.logs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.mutable.MutableDouble;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.util.KeyValPair;

/**
 * <p>
 * MultiWindowDimensionAggregation class.
 * </p>
 * This class aggregates the value of given dimension across windows
 * 
 * @since 0.3.4
 */
public class MultiWindowDimensionAggregation implements Operator
{

  private static final Logger logger = LoggerFactory.getLogger(MultiWindowDimensionAggregation.class);

  public enum AggregateOperation {
    SUM, AVERAGE
  };

  private int windowSize = 2;
  private int currentWindow = 0;
  private List<int[]> dimensionArray;
  private String timeBucket = "m";
  private List<Pattern> patternList;
  private String dimensionKeyVal = "0";
  private List<String> dimensionArrayString;
  private AggregateOperation operationType = AggregateOperation.SUM;
  private int applicationWindowSize = 500;

  // map(currentWindow, map(dimensionArryString.getIndex(), map(dimension value
  // , (sum of values, number of values))))
  private Map<Integer, Map<String, Map<String, KeyValPair<MutableDouble, Integer>>>> cacheOject;

  public final transient DefaultOutputPort<Map<String, DimensionObject<String>>> output = new DefaultOutputPort<Map<String, DimensionObject<String>>>();

  public final transient DefaultInputPort<Map<String, Map<String, Number>>> data = new DefaultInputPort<Map<String, Map<String, Number>>>() {
    @Override
    public void process(Map<String, Map<String, Number>> tuple)
    {

      Map<String, Map<String, KeyValPair<MutableDouble, Integer>>> currentWindowMap = cacheOject.get(currentWindow);
      if (currentWindowMap == null) {
        currentWindowMap = new HashMap<String, Map<String, KeyValPair<MutableDouble, Integer>>>();
        cacheOject.put(currentWindow, currentWindowMap);
      }

      for (Map.Entry<String, Map<String, Number>> tupleEntry : tuple.entrySet()) {
        // System.out.println(tupleEntry);
        String tupleKey = tupleEntry.getKey();
        Map<String, Number> tupleValue = tupleEntry.getValue();
        int currentPattern = 0;
        for (Pattern pattern : patternList) {
          Matcher matcher = pattern.matcher(tupleKey);
         // logger.info(" key getting matched {}", tupleKey);
        //  logger.info("pattern matched against {}", pattern.pattern());
          if (matcher.matches()) {
           // logger.info("pattern matched against {}", pattern.pattern());
            Map<String, KeyValPair<MutableDouble, Integer>> currentDimensionArrayMap = currentWindowMap.get(dimensionArrayString.get(currentPattern));
            if (currentDimensionArrayMap == null) {
              currentDimensionArrayMap = new HashMap<String, KeyValPair<MutableDouble, Integer>>();
              currentWindowMap.put(dimensionArrayString.get(currentPattern), currentDimensionArrayMap);
            }
            StringBuilder builder = new StringBuilder(matcher.group(2));
            for (int i = 1; i < dimensionArray.get(currentPattern).length; i++) {
              builder.append("," + matcher.group(i + 2));
            }
            KeyValPair<MutableDouble, Integer> keyValPair = currentDimensionArrayMap.get(builder.toString());
            if (keyValPair == null) {
              keyValPair = new KeyValPair<MutableDouble, Integer>(((MutableDouble) tupleValue.get(dimensionKeyVal)), new Integer(1));
              currentDimensionArrayMap.put(builder.toString(), keyValPair);
            } else {
              MutableDouble key = keyValPair.getKey();
              key.add(tupleValue.get(dimensionKeyVal));
              int count = keyValPair.getValue() + 1;
              keyValPair = new KeyValPair<MutableDouble, Integer>(key, new Integer(count));
              currentDimensionArrayMap.put(builder.toString(), keyValPair);
            }
            break;
          }
          currentPattern++;
        }

      }
    }
  };

  public String getDimensionKeyVal()
  {
    return dimensionKeyVal;
  }

  public void setDimensionKeyVal(String dimensionKeyVal)
  {
    this.dimensionKeyVal = dimensionKeyVal;
  }

  public String getTimeBucket()
  {
    return timeBucket;
  }

  public void setTimeBucket(String timeBucket)
  {
    this.timeBucket = timeBucket;
  }

  public List<int[]> getDimensionArray()
  {
    return dimensionArray;
  }

  public void setDimensionArray(List<int[]> dimensionArray)
  {
    this.dimensionArray = dimensionArray;
    dimensionArrayString = new ArrayList<String>();
    for (int[] e : dimensionArray) {
      StringBuilder builder = new StringBuilder("" + e[0]);
      for (int i = 1; i < e.length; i++) {
        builder.append("," + e[i]);
      }
      dimensionArrayString.add(builder.toString());

    }

  }

  public int getWindowSize()
  {
    return windowSize;
  }

  public void setWindowSize(int windowSize)
  {
    this.windowSize = windowSize;
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    if (arg0 != null)
      applicationWindowSize = arg0.attrValue(OperatorContext.APPLICATION_WINDOW_COUNT, 500);
    cacheOject = new HashMap<Integer, Map<String, Map<String, KeyValPair<MutableDouble, Integer>>>>(windowSize);
    setUpPatternList();
    // System.out.println("size of various lists " + dimensionArrayString.size()
    // + " " +patternList.size());
  }

  private void setUpPatternList()
  {
    patternList = new ArrayList<Pattern>();
    for (int[] e : dimensionArray) {
      Pattern pattern;
      StringBuilder builder = new StringBuilder(timeBucket + "\\|(\\d+)");
      for (int i = 0; i < e.length; i++) {
        //builder.append("\\|" + e[i] + ":(\\S+)");
        builder.append("\\|" + e[i] + ":([^\\|]+)");
      }
       
      // System.out.println(builder.toString());
      pattern = Pattern.compile(builder.toString());
      patternList.add(pattern);
    }
  }

  @Override
  public void teardown()
  {

  }

  @Override
  public void beginWindow(long arg0)
  {
    Map<String, Map<String, KeyValPair<MutableDouble, Integer>>> currentWindowMap = cacheOject.get(currentWindow);
    if (currentWindowMap == null) {
      currentWindowMap = new HashMap<String, Map<String, KeyValPair<MutableDouble, Integer>>>();
    }
    currentWindowMap.clear();
    if (patternList == null || patternList.isEmpty())
      setUpPatternList();

  }

  @Override
  public void endWindow()
  {

    Map<String, Map<String, KeyValPair<MutableDouble, Integer>>> outputMap = new HashMap<String, Map<String, KeyValPair<MutableDouble, Integer>>>();

    System.out.println(cacheOject);
    Collection<Map<String, Map<String, KeyValPair<MutableDouble, Integer>>>> coll = cacheOject.values();
    int totalWindowsOccupied = coll.size();
    for (Map<String, Map<String, KeyValPair<MutableDouble, Integer>>> e : coll) {
      for (String dimension : dimensionArrayString) {
        if (e.get(dimension) != null) {
          Map<String, KeyValPair<MutableDouble, Integer>> dimensionMap = outputMap.get(dimension);
          if (dimensionMap == null) {
            Map<String, KeyValPair<MutableDouble, Integer>> eClone = new HashMap<String, KeyValPair<MutableDouble,Integer>>();
            Map<String, KeyValPair<MutableDouble, Integer>> cloneCopy = e.get(dimension);
            for(Map.Entry<String, KeyValPair<MutableDouble, Integer>> cloneCopyTuple : cloneCopy.entrySet()){
              KeyValPair<MutableDouble, Integer> toCopyKeyValPair = cloneCopyTuple.getValue();
              eClone.put(cloneCopyTuple.getKey(),new KeyValPair<MutableDouble, Integer>(new MutableDouble(toCopyKeyValPair.getKey()), toCopyKeyValPair.getValue().intValue()));
            }
            outputMap.put(dimension,eClone);
          } else {
            Map<String, KeyValPair<MutableDouble, Integer>> currentCacheObject = e.get(dimension);
            Set<String> keys = currentCacheObject.keySet();
            for (String key : keys) {
              if (dimensionMap.get(key) == null) {
                KeyValPair<MutableDouble,Integer> toCloneKeyValPair = currentCacheObject.get(key);
                dimensionMap.put(key, new KeyValPair<MutableDouble, Integer>(new MutableDouble(toCloneKeyValPair.getKey()), toCloneKeyValPair.getValue()));
              } else {
                KeyValPair<MutableDouble, Integer> dimensionKeyVal = dimensionMap.get(key);
                KeyValPair<MutableDouble, Integer> currentCacheObjectKeyVal = currentCacheObject.get(key);
                MutableDouble count = dimensionKeyVal.getKey();
                count.add(currentCacheObjectKeyVal.getKey());
                dimensionKeyVal.setValue(dimensionKeyVal.getValue() + currentCacheObjectKeyVal.getValue());
              }
            }
          }
        }
      }
    }
    // System.out.println("inside end window of multiwindow" + outputMap);
   
    for (Map.Entry<String, Map<String, KeyValPair<MutableDouble, Integer>>> e : outputMap.entrySet()) {
      for (Map.Entry<String, KeyValPair<MutableDouble, Integer>> dimensionValObj : e.getValue().entrySet()) {
        Map<String, DimensionObject<String>> outputData = new HashMap<String, DimensionObject<String>>();
        KeyValPair<MutableDouble, Integer> keyVal = dimensionValObj.getValue();

        if (operationType == AggregateOperation.SUM) {
          outputData.put(e.getKey(), new DimensionObject<String>(keyVal.getKey(), dimensionValObj.getKey()));
        } else if (operationType == AggregateOperation.AVERAGE) {
          double totalCount = ((double) (totalWindowsOccupied * keyVal.getValue() * applicationWindowSize)) / 1000;
          outputData.put(e.getKey(), new DimensionObject<String>(new MutableDouble(keyVal.getKey().doubleValue() / totalCount), dimensionValObj.getKey()));
        }
        output.emit(outputData);    
      }
    }
    
    currentWindow = (currentWindow + 1) % windowSize;

  }

  public AggregateOperation getOperationType()
  {
    return operationType;
  }

  public void setOperationType(AggregateOperation operationType)
  {
    this.operationType = operationType;
  }

}
