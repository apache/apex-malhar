package com.datatorrent.lib.stream;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.KeyValPair;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author davidyan
 */
public class ConsolidatorKeyVal<K,V1,V2,V3,V4,V5> implements Operator
{
  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  public class ConsolidatorInputPort<V> extends DefaultInputPort<KeyValPair<K, V>>
  {
    private int number;

    ConsolidatorInputPort(Operator oper, int num)
    {
      super();
      number = num;
    }

    @Override
    public void process(KeyValPair<K, V> tuple)
    {
      K key = tuple.getKey();
      ArrayList<Object> list = getObject(key);
      list.set(number, tuple.getValue());
    }

  }

  protected HashMap<K, ArrayList<Object>> result;
  // We now only support up to 5 input ports.
  // Ideally we should use use an array of input ports when it's supported, like this:
  //public transient ArrayList<ConsolidatorInputPort> inputs;
  //
  //protected int numOfInputs;
  @InputPortFieldAnnotation(name = "in1")
  public final transient ConsolidatorInputPort<V1> in1 = new ConsolidatorInputPort<V1>(this, 0);
  @InputPortFieldAnnotation(name = "in2")
  public final transient ConsolidatorInputPort<V2> in2 = new ConsolidatorInputPort<V2>(this, 1);
  @InputPortFieldAnnotation(name = "in3", optional = true)
  public final transient ConsolidatorInputPort<V3> in3 = new ConsolidatorInputPort<V3>(this, 2);
  @InputPortFieldAnnotation(name = "in4", optional = true)
  public final transient ConsolidatorInputPort<V4> in4 = new ConsolidatorInputPort<V4>(this, 3);
  @InputPortFieldAnnotation(name = "in5", optional = true)
  public final transient ConsolidatorInputPort<V5> in5 = new ConsolidatorInputPort<V5>(this, 4);
  @OutputPortFieldAnnotation(name = "out")
  public final transient DefaultOutputPort<HashMap<K, ArrayList<Object>>> out = new DefaultOutputPort<HashMap<K, ArrayList<Object>>>();

  public ArrayList<Object> getObject(K k)
  {
    ArrayList<Object> val = result.get(k);
    if (val == null) {
      val = new ArrayList<Object>(5);
      val.add(0, null);
      val.add(1, null);
      val.add(2, null);
      val.add(3, null);
      val.add(4, null);
      result.put(k, val);
    }
    return val;
  }

  @Override
  public void beginWindow(long windowId)
  {
    result = new HashMap<K, ArrayList<Object>>();
  }

  /**
   * Emits merged data
   */
  @Override
  public void endWindow()
  {
    if (!result.isEmpty()) {
      out.emit(result);
    }
  }

}
