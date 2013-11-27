package com.datatorrent.contrib.machinedata.operator;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.contrib.machinedata.data.AverageData;
import com.datatorrent.contrib.machinedata.data.MachineKey;
import com.datatorrent.lib.util.KeyHashValPair;


/**
 * This class calculates the partial sum and count for a given key
 * <p>MachineInfoAveragingUnifier class.</p>
 *
 * @since 0.9.0
 */
public class MachineInfoAveragingUnifier implements Unifier<KeyHashValPair<MachineKey, Map<String, AverageData>>>
{

  private Map<MachineKey, Map<String, AverageData>> sums = new HashMap<MachineKey, Map<String, AverageData>>();
  public final transient DefaultOutputPort<KeyHashValPair<MachineKey, Map<String, AverageData>>> outputPort = new DefaultOutputPort<KeyHashValPair<MachineKey, Map<String, AverageData>>>();

  @Override
  public void beginWindow(long arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<MachineKey, Map<String, AverageData>> entry : sums.entrySet()) {
      outputPort.emit(new KeyHashValPair<MachineKey, Map<String, AverageData>>(entry.getKey(), entry.getValue()));
    }
    sums.clear();
  
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void process(KeyHashValPair<MachineKey, Map<String, AverageData>> arg0)
  {
    MachineKey tupleKey = arg0.getKey();
    Map<String, AverageData> sumsMap = sums.get(tupleKey);
    Map<String, AverageData> tupleValue = arg0.getValue();
    if (sumsMap == null) {
      sums.put(tupleKey, tupleValue);
    } else {
      updateSum("cpu", sumsMap, tupleValue);
      updateSum("ram", sumsMap, tupleValue);
      updateSum("hdd", sumsMap, tupleValue);
    }

  }

  /**
   * This method updates the sum and count for a given Resource key
   * @param resourceKey the resource key whose sum and count needs to be updated
   * @param sumsMap the map that stores the sum and count values
   * @param tupleMap the new tuple map that is added
   */
  private void updateSum(String resourceKey, Map<String, AverageData> sumsMap, Map<String, AverageData> tupleMap)
  {
    AverageData sumsAverageData = sumsMap.get(resourceKey);
    AverageData tupleAverageData = tupleMap.get(resourceKey);
    if (tupleAverageData != null) {
      if (sumsAverageData != null) {
        sumsAverageData.setCount(sumsAverageData.getCount() + tupleAverageData.getCount());
        sumsAverageData.setSum(sumsAverageData.getSum() + tupleAverageData.getSum());
      } else {
        sumsMap.put(resourceKey, tupleAverageData);
      }
    }

  }

}
