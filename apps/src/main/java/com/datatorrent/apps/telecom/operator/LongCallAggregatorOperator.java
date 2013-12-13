package com.datatorrent.apps.telecom.operator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;

public class LongCallAggregatorOperator implements Operator
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
  private Map<Integer, Map<String, Map<String, String>>> windowCacheObject;

  public final transient DefaultOutputPort<Map<String, String>> output = new DefaultOutputPort<Map<String, String>>();

  public final transient DefaultInputPort<Map<String, String>> input = new DefaultInputPort<Map<String, String>>() {
    @Override
    public void process(Map<String, String> t)
    {
      // Identifying if it is Mergee Object
      if (isMergeeObject(t)) {
        Map<String, Map<String, String>> currentWindowMap = windowCacheObject.get(currentWindow);
        String matchFieldId = getMatchFieldString(t);
        if (matchFieldId == null) {
          return;
        }
        currentWindowMap.put(matchFieldId, t);
        return;
      }

      // Identifying if it is Main Object
      if (isAcquirer(t)) {
        // iterating over the window size to find the mergee objects
        String matchFieldId = getMatchFieldString(t);
        if (matchFieldId != null) {
          for (int i = 0; i < windowSize; i++) {
            Map<String, Map<String, String>> currentWindowMap = windowCacheObject.get(i);
            currentWindowMap.remove(matchFieldId);
          }
        }
      }

      output.emit(t);

    }

    private boolean isAcquirer(Map<String, String> t)
    {
      for (Map.Entry<String, String> entry : acquirerIdentifier.entrySet()) {
        if (t.get(entry.getKey()) == null || !(t.get(entry.getKey()).equalsIgnoreCase(entry.getValue()))) {
          return false;
        }
      }
      return true;
    }

    private String getMatchFieldString(Map<String, String> t)
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

    private boolean isMergeeObject(Map<String, String> t)
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
      windowCacheObject = new HashMap<Integer, Map<String, Map<String, String>>>(windowSize);
    }

  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    Map<String, Map<String, String>> currentWindowMap = windowCacheObject.get(currentWindow);
    if (currentWindowMap == null) {
      currentWindowMap = new HashMap<String, Map<String, String>>();
      windowCacheObject.put(currentWindow, currentWindowMap);
    } else {
      currentWindowMap.clear();
    }

  }

  @Override
  public void endWindow()
  {
    currentWindow = (currentWindow + 1) % windowSize;
    if (windowCacheObject.size() == windowSize) {
      Map<String, Map<String, String>> currentWindowMap = windowCacheObject.get(currentWindow);
      for (Map.Entry<String, Map<String, String>> entry : currentWindowMap.entrySet()) {
        output.emit(entry.getValue());
      }
    }
    

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

  public int getWindowSize()
  {
    return windowSize;
  }

  public void setWindowSize(int windowSize)
  {
    this.windowSize = windowSize;
  }

}
