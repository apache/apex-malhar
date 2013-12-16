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

package com.datatorrent.apps.telecom.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;

/**
 * <p>CallForwardingAggregatorOperator class.</p>
 *
 * @since 0.9.2
 */
public class CallForwardingAggregatorOperator implements Operator
{

  /**
   * This map represents the fields and their values that will be used to identity the main object which will add fields
   * from objects marked as mergee
   */
  @NotNull
  Map<String, String> acquirerIdentifier;

  /**
   * This map represents the fields and their values that will be used to identity the Mergee object whose fields will
   * be added to objects identified as main
   */
  @NotNull
  Map<String, String> mergeeIdentifier;

  /**
   * This list contains the fields that will be used to match the Main Acquirer object with Mergee object
   */
  @NotNull
  List<String> matchFieldList;

  /**
   * This list contains the fields that need to be added to Acquiere object from Merge object
   */
  @NotNull
  List<String> mergeFieldList;

  /**
   * This represents the sliding window count
   */
  @NotNull
  private int windowSize = 2;
  /**
   * This is used for adding values in cache object in cyclic manner
   */
  private int currentWindow = 0;

  /**
   * This stores the list of all the Mergee objects that come in a particular window
   */
  private Map<Integer, Map<String, List<Map<String, String>>>> windowCacheObject;

  public final transient DefaultOutputPort<HashMap<String, String>> output = new DefaultOutputPort<HashMap<String, String>>();

  public final transient DefaultInputPort<HashMap<String, String>> input = new DefaultInputPort<HashMap<String, String>>() {
    @Override
    public void process(HashMap<String, String> t)
    {
      // Identifying if it is Mergee Object
      if (isMergeeObject(t)) {
        Map<String, List<Map<String, String>>> currentWindowMap = windowCacheObject.get(currentWindow);
        String matchFieldId = getMatchFieldString(t);
        if (matchFieldId == null) {
          return;
        }
        List<Map<String, String>> list = currentWindowMap.get(matchFieldId);
        if (list == null) {
          list = new ArrayList<Map<String, String>>();
          list.add(t);
          currentWindowMap.put(matchFieldId, list);
        } else {
          list.add(t);
        }
        return;
      }

      // Identifying if it is Main Object
      if (isAcquirer(t)) {
        // iterating over the window size to find the mergee objects
        String matchFieldId = getMatchFieldString(t);
        if (matchFieldId != null) {
          for (int i = 0; i < windowSize; i++) {
            Map<String, List<Map<String, String>>> currentWindowMap = windowCacheObject.get(i);
            if (currentWindowMap.get(matchFieldId) != null) {
              List<Map<String, String>> list = currentWindowMap.get(matchFieldId);
              Iterator<Map<String, String>> itr = list.iterator();
              while (itr.hasNext()) {
                Map<String, String> m = itr.next();
                Iterator<String> mergeFieldItr = mergeFieldList.iterator();
                while (mergeFieldItr.hasNext()) {
                  String mergeField = mergeFieldItr.next();
                  if (m.get(mergeField) != null) {
                    t.put(mergeField, m.get(mergeField));
                  }
                }
              }
              // removing it as it is no longer needed
              currentWindowMap.remove(matchFieldId);
            }
          }
        }
      }

      output.emit(t);

    }

    private boolean isAcquirer(HashMap<String, String> t)
    {
      for (Map.Entry<String, String> entry : acquirerIdentifier.entrySet()) {
        if (t.get(entry.getKey()) == null || !(t.get(entry.getKey()).equalsIgnoreCase(entry.getValue()))) {
          return false;
        }
      }
      return true;
    }

    private String getMatchFieldString(HashMap<String, String> t)
    {
      StringBuilder output = new StringBuilder();
      Iterator<String> itr = matchFieldList.iterator();
      while (itr.hasNext()) {
        String matchField = itr.next();
        if (t.get(matchField) != null) {
          output.append(t.get(matchField)).append("-");
        }
      }
      if (output.length() < 1)
        return null;
      return output.toString();
    }

    private boolean isMergeeObject(HashMap<String, String> t)
    {
      for (Map.Entry<String, String> entry : mergeeIdentifier.entrySet()) {
        if (t.get(entry.getKey()) == null || !(t.get(entry.getKey()).equalsIgnoreCase(entry.getValue()))) {
          return false;
        }
      }
      return true;
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    if (windowCacheObject == null) {
      windowCacheObject = new HashMap<Integer, Map<String, List<Map<String, String>>>>(windowSize);
    }

  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    Map<String, List<Map<String, String>>> currentWindowMap = windowCacheObject.get(currentWindow);
    if (currentWindowMap == null) {
      currentWindowMap = new HashMap<String, List<Map<String, String>>>();
      windowCacheObject.put(currentWindow, currentWindowMap);
    } else {
      currentWindowMap.clear();
    }

  }

  @Override
  public void endWindow()
  {
    currentWindow = (currentWindow + 1) % windowSize;

  }

  public Map<String, String> getAcquirerIdentifier()
  {
    return acquirerIdentifier;
  }

  public void setAcquirerIdentifier(Map<String, String> acquirerIdentifier)
  {
    this.acquirerIdentifier = acquirerIdentifier;
  }

  public Map<String, String> getMergeeIdentifier()
  {
    return mergeeIdentifier;
  }

  public void setMergeeIdentifier(Map<String, String> mergeeIdentifier)
  {
    this.mergeeIdentifier = mergeeIdentifier;
  }

  public List<String> getMatchFieldList()
  {
    return matchFieldList;
  }

  public void setMatchFieldList(List<String> matchFieldList)
  {
    this.matchFieldList = matchFieldList;
  }

  public List<String> getMergeFieldList()
  {
    return mergeFieldList;
  }

  public void setMergeFieldList(List<String> mergeFieldList)
  {
    this.mergeFieldList = mergeFieldList;
  }

  public int getWindowSize()
  {
    return windowSize;
  }

  public void setWindowSize(int windowSize)
  {
    this.windowSize = windowSize;
  }

  
}
