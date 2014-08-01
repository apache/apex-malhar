package com.datatorrent.demos.machinedata.operator;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

import com.datatorrent.demos.machinedata.data.AverageData;
import com.datatorrent.demos.machinedata.data.MachineKey;
import com.datatorrent.lib.util.KeyHashValPair;

/**
 * This class calculates the partial sum and count for a given key
 * <p>MachineInfoAveragingUnifier class.</p>
 *
 * @since 0.9.0
 */
public class MachineInfoAveragingUnifier implements Unifier<KeyHashValPair<MachineKey, AverageData>>
{

  private Map<MachineKey, AverageData> sums = new HashMap<MachineKey, AverageData>();
  public final transient DefaultOutputPort<KeyHashValPair<MachineKey, AverageData>> outputPort = new DefaultOutputPort<KeyHashValPair<MachineKey, AverageData>>();

  @Override
  public void beginWindow(long arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<MachineKey, AverageData> entry : sums.entrySet()) {
      outputPort.emit(new KeyHashValPair<MachineKey, AverageData>(entry.getKey(), entry.getValue()));
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
  public void process(KeyHashValPair<MachineKey, AverageData> arg0)
  {
    MachineKey tupleKey = arg0.getKey();
    AverageData averageData = sums.get(tupleKey);
    AverageData tupleValue = arg0.getValue();
    if (averageData == null) {
      sums.put(tupleKey, tupleValue);
    }
    else {
      averageData.setCpu(averageData.getCpu() + tupleValue.getCpu());
      averageData.setRam(averageData.getRam() + tupleValue.getRam());
      averageData.setHdd(averageData.getHdd() + tupleValue.getHdd());
      averageData.setCount(averageData.getCount() + tupleValue.getCount());
    }
  }

}
