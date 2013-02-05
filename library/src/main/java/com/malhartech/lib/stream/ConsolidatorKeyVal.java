package com.malhartech.lib.stream;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator;
import com.malhartech.lib.util.KeyValPair;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author davidyan
 */
public class ConsolidatorKeyVal<K> implements Operator
{
  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  public class ConsolidatorInputPort extends DefaultInputPort<KeyValPair<K, Object>>
  {
    private int number;

    ConsolidatorInputPort(Operator oper, int num)
    {
      super(oper);
      number = num;
    }

    @Override
    public void process(KeyValPair<K, Object> tuple)
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
  public transient ConsolidatorInputPort in1 = new ConsolidatorInputPort(this, 0);
  @InputPortFieldAnnotation(name = "in2")
  public transient ConsolidatorInputPort in2 = new ConsolidatorInputPort(this, 1);
  @InputPortFieldAnnotation(name = "in3", optional = true)
  public transient ConsolidatorInputPort in3 = new ConsolidatorInputPort(this, 2);
  @InputPortFieldAnnotation(name = "in4", optional = true)
  public transient ConsolidatorInputPort in4 = new ConsolidatorInputPort(this, 3);
  @InputPortFieldAnnotation(name = "in5", optional = true)
  public transient ConsolidatorInputPort in5 = new ConsolidatorInputPort(this, 4);
  @OutputPortFieldAnnotation(name = "out")
  public final transient DefaultOutputPort<HashMap<K, ArrayList<Object>>> out = new DefaultOutputPort<HashMap<K, ArrayList<Object>>>(this);

  public ArrayList<Object> getObject(K k)
  {
    ArrayList<Object> val = result.get(k);
    if (val == null) {
      val = new ArrayList<Object>(5);
      val.add(0, null);
      val.add(1, null);
      if (in3.isConnected())
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
