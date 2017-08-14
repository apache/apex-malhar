/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.logs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * This class aggregates the value of given dimension across windows.
 * <p></p>
 * @displayName Multi Window Dimension Aggregation
 * @category Stats and Aggregations
 * @tags aggregation
 *
 * @since 0.3.4
 */
public class MultiWindowDimensionAggregation implements Operator
{

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(MultiWindowDimensionAggregation.class);

  public enum AggregateOperation
  {
    SUM, AVERAGE
  }

  private int windowSize = 2;
  private int currentWindow = 0;
  private String timeBucket = "m";
  private String dimensionKeyVal = "0";
  private List<String> dimensionArrayString;
  @NotNull
  private List<int[]> dimensionArray;
  private AggregateOperation operationType = AggregateOperation.SUM;
  private Map<String, Map<String, KeyValPair<MutableDouble, Integer>>> outputMap;
  private Map<Integer, Map<String, Map<String, Number>>> cacheOject;

  private transient List<Pattern> patternList;
  private transient int applicationWindowSize = 500;
  /**
   * This is the output port which emits aggregated dimensions.
   */
  public final transient DefaultOutputPort<Map<String, DimensionObject<String>>> output = new DefaultOutputPort<Map<String, DimensionObject<String>>>();
  /**
   * This is the input port which receives multi dimensional data.
   */
  public final transient DefaultInputPort<Map<String, Map<String, Number>>> data = new DefaultInputPort<Map<String, Map<String, Number>>>()
  {
    @Override
    public void process(Map<String, Map<String, Number>> tuple)
    {
      cacheOject.put(currentWindow, tuple);
      for (Map.Entry<String, Map<String, Number>> tupleEntry : tuple.entrySet()) {
        String tupleKey = tupleEntry.getKey();
        Map<String, Number> tupleValue = tupleEntry.getValue();
        int currentPattern = 0;
        for (Pattern pattern : patternList) {
          Matcher matcher = pattern.matcher(tupleKey);
          if (matcher.matches()) {
            String currentPatternString = dimensionArrayString.get(currentPattern);
            Map<String, KeyValPair<MutableDouble, Integer>> currentPatternMap = outputMap.get(currentPatternString);
            if (currentPatternMap == null) {
              currentPatternMap = new HashMap<String, KeyValPair<MutableDouble, Integer>>();
              outputMap.put(currentPatternString, currentPatternMap);
            }
            StringBuilder builder = new StringBuilder(matcher.group(2));
            for (int i = 1; i < dimensionArray.get(currentPattern).length; i++) {
              builder.append("," + matcher.group(i + 2));
            }

            KeyValPair<MutableDouble, Integer> currentDimensionKeyValPair = currentPatternMap.get(builder.toString());
            if (currentDimensionKeyValPair == null) {
              currentDimensionKeyValPair = new KeyValPair<MutableDouble, Integer>(new MutableDouble(tupleValue.get(dimensionKeyVal)), 1);
              currentPatternMap.put(builder.toString(), currentDimensionKeyValPair);
            } else {
              currentDimensionKeyValPair.getKey().add(tupleValue.get(dimensionKeyVal));
              currentDimensionKeyValPair.setValue(currentDimensionKeyValPair.getValue() + 1);
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
    if (arg0 != null) {
      applicationWindowSize = arg0.getValue(OperatorContext.APPLICATION_WINDOW_COUNT);
    }
    if (cacheOject == null) {
      cacheOject = new HashMap<Integer, Map<String, Map<String, Number>>>(windowSize);
    }
    if (outputMap == null) {
      outputMap = new HashMap<String, Map<String, KeyValPair<MutableDouble, Integer>>>();
    }
    setUpPatternList();

  }

  private void setUpPatternList()
  {
    patternList = new ArrayList<Pattern>();
    for (int[] e : dimensionArray) {
      Pattern pattern;
      StringBuilder builder = new StringBuilder(timeBucket + "\\|(\\d+)");
      for (int i = 0; i < e.length; i++) {
        builder.append("\\|" + e[i] + ":([^\\|]+)");
      }

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
    Map<String, Map<String, Number>> currentWindowMap = cacheOject.get(currentWindow);
    if (currentWindowMap == null) {
      currentWindowMap = new HashMap<String, Map<String, Number>>();
    } else {
      for (Map.Entry<String, Map<String, Number>> tupleEntry : currentWindowMap.entrySet()) {
        String tupleKey = tupleEntry.getKey();
        Map<String, Number> tupleValue = tupleEntry.getValue();
        int currentPattern = 0;
        for (Pattern pattern : patternList) {
          Matcher matcher = pattern.matcher(tupleKey);
          if (matcher.matches()) {
            String currentPatternString = dimensionArrayString.get(currentPattern);
            Map<String, KeyValPair<MutableDouble, Integer>> currentPatternMap = outputMap.get(currentPatternString);
            if (currentPatternMap != null) {
              StringBuilder builder = new StringBuilder(matcher.group(2));
              for (int i = 1; i < dimensionArray.get(currentPattern).length; i++) {
                builder.append("," + matcher.group(i + 2));
              }
              KeyValPair<MutableDouble, Integer> currentDimensionKeyValPair = currentPatternMap.get(builder.toString());
              if (currentDimensionKeyValPair != null) {
                currentDimensionKeyValPair.getKey().add(0 - tupleValue.get(dimensionKeyVal).doubleValue());
                currentDimensionKeyValPair.setValue(currentDimensionKeyValPair.getValue() - 1);
                if (currentDimensionKeyValPair.getKey().doubleValue() == 0.0) {
                  currentPatternMap.remove(builder.toString());
                }
              }
            }
            break;
          }
          currentPattern++;
        }

      }
    }
    currentWindowMap.clear();
    if (patternList == null || patternList.isEmpty()) {
      setUpPatternList();
    }

  }

  @Override
  public void endWindow()
  {
    int totalWindowsOccupied = cacheOject.size();
    for (Map.Entry<String, Map<String, KeyValPair<MutableDouble, Integer>>> e : outputMap.entrySet()) {
      for (Map.Entry<String, KeyValPair<MutableDouble, Integer>> dimensionValObj : e.getValue().entrySet()) {
        Map<String, DimensionObject<String>> outputData = new HashMap<String, DimensionObject<String>>();
        KeyValPair<MutableDouble, Integer> keyVal = dimensionValObj.getValue();
        if (operationType == AggregateOperation.SUM) {
          outputData.put(e.getKey(), new DimensionObject<String>(keyVal.getKey(), dimensionValObj.getKey()));
        } else if (operationType == AggregateOperation.AVERAGE) {
          if (keyVal.getValue() != 0) {
            double totalCount = ((double)(totalWindowsOccupied * applicationWindowSize)) / 1000;
            outputData.put(e.getKey(), new DimensionObject<String>(new MutableDouble(keyVal.getKey().doubleValue() / totalCount), dimensionValObj.getKey()));
          }
        }
        if (!outputData.isEmpty()) {
          output.emit(outputData);
        }
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
