package com.datatorrent.lib.algo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

/**
 * This unifier is used by the LeastFrequentKey operator in order to merge the output of the "list" output port.
 * <p></p>
 * @displayName Emit Least Frequent Value
 * @category algorithm
 * @tags filter, count
 *
 * @since 0.3.3
 */
public class LeastFrequentKeyArrayUnifier<K> implements Unifier<ArrayList<HashMap<K, Integer>>>
{
  /**
   * Key/Least value map.
   */
  private HashMap<K, Integer> leastMap  = new HashMap<K, Integer>();

  /**
   * The output port on which the least frequent tuple is emitted.
   */
  public final transient DefaultOutputPort<HashMap<K, Integer>> mergedport = new DefaultOutputPort<HashMap<K, Integer>>();

  @Override
  public void beginWindow(long arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    mergedport.emit(leastMap);
    leastMap.clear();
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
  public void process(ArrayList<HashMap<K, Integer>> tuples)
  {
    for (HashMap<K, Integer> tuple : tuples) {
      for (Map.Entry<K, Integer> entry : tuple.entrySet()) {
        Integer value = entry.getValue();
        if (leastMap.containsKey(entry.getKey())) {
          if (leastMap.get(entry.getKey()) < value) {
            value = leastMap.remove(entry.getKey());
          } else {
            leastMap.remove(entry.getKey());
          }
        }
        leastMap.put(entry.getKey(), value);
      }
    }
  }

}
