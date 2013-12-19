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
public class CallForwardingAggregatorOperator<K,V> implements Operator
{

  /**
   * This map represents the fields and their values that will be used to identity the main object which will add fields
   * from objects marked as mergee
   */
  @NotNull
  Map<K, V> acquirerIdentifier;

  /**
   * This map represents the fields and their values that will be used to identity the Mergee object whose fields will
   * be added to objects identified as main
   */
  @NotNull
  Map<K, V> mergeeIdentifier;

  /**
   * This list contains the fields that will be used to match the Main Acquirer object with Mergee object
   */
  @NotNull
  List<K> matchFieldList;

  /**
   * This list contains the fields that need to be added to Acquiere object from Merge object
   */
  @NotNull
  List<K> mergeFieldList;

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
  private Map<Integer, Map<String, List<Map<K, V>>>> windowCacheObject;

  public final transient DefaultOutputPort<Map<K, V>> output = new DefaultOutputPort<Map<K, V>>();

  public final transient DefaultInputPort<Map<K, V>> input = new DefaultInputPort<Map<K, V>>() {
    @Override
    public void process(Map<K, V> t)
    {
      // Identifying if it is Mergee Object
      if (isMergeeObject(t)) {
        Map<String, List<Map<K, V>>> currentWindowMap = windowCacheObject.get(currentWindow);
        String matchFieldId = getMatchFieldString(t);
        if (matchFieldId == null) {
          return;
        }
        List<Map<K, V>> list = currentWindowMap.get(matchFieldId);
        if (list == null) {
          list = new ArrayList<Map<K, V>>();
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
            Map<String, List<Map<K, V>>> currentWindowMap = windowCacheObject.get(i);
            if (currentWindowMap!= null && currentWindowMap.get(matchFieldId) != null) {
              List<Map<K, V>> list = currentWindowMap.get(matchFieldId);
              Iterator<Map<K, V>> itr = list.iterator();
              while (itr.hasNext()) {
                Map<K, V> m = itr.next();
                Iterator<K> mergeFieldItr = mergeFieldList.iterator();
                while (mergeFieldItr.hasNext()) {
                  K mergeField = mergeFieldItr.next();
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

    private boolean isAcquirer(Map<K, V> t)
    {
      for (Map.Entry<K, V> entry : acquirerIdentifier.entrySet()) {
        if (t.get(entry.getKey()) == null || !(t.get(entry.getKey()).equals(entry.getValue()))) {
          return false;
        }
      }
      return true;
    }

    private String getMatchFieldString(Map<K, V> t)
    {
      StringBuilder output = new StringBuilder();
      Iterator<K> itr = matchFieldList.iterator();
      while (itr.hasNext()) {
        K matchField = itr.next();
        if (t.get(matchField) != null) {
          output.append(t.get(matchField)).append("-");
        }
      }
      if (output.length() < 1)
        return null;
      return output.toString();
    }

    private boolean isMergeeObject(Map<K, V> t)
    {
      for (Map.Entry<K, V> entry : mergeeIdentifier.entrySet()) {
        if (t.get(entry.getKey()) == null || !(t.get(entry.getKey()).equals(entry.getValue()))) {
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
      windowCacheObject = new HashMap<Integer, Map<String, List<Map<K, V>>>>(windowSize);
    }

  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    Map<String, List<Map<K, V>>> currentWindowMap = windowCacheObject.get(currentWindow);
    if (currentWindowMap == null) {
      currentWindowMap = new HashMap<String, List<Map<K, V>>>();
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

  public Map<K, V> getAcquirerIdentifier()
  {
    return acquirerIdentifier;
  }

  public void setAcquirerIdentifier(Map<K, V> acquirerIdentifier)
  {
    this.acquirerIdentifier = acquirerIdentifier;
  }

  public Map<K, V> getMergeeIdentifier()
  {
    return mergeeIdentifier;
  }

  public void setMergeeIdentifier(Map<K, V> mergeeIdentifier)
  {
    this.mergeeIdentifier = mergeeIdentifier;
  }

  public List<K> getMatchFieldList()
  {
    return matchFieldList;
  }

  public void setMatchFieldList(List<K> matchFieldList)
  {
    this.matchFieldList = matchFieldList;
  }

  public List<K> getMergeFieldList()
  {
    return mergeFieldList;
  }

  public void setMergeFieldList(List<K> mergeFieldList)
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
