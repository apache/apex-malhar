package com.datatorrent.lib.streamquery;

import java.util.ArrayList;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

public class SelectTopOperator<T> implements Operator
{
  private ArrayList<T> list;
  private int topValue = 1;
  private boolean isPercentage = false;
  
  /**
   * Input port.
   */
  public final transient DefaultInputPort<T> inport = new DefaultInputPort<T>() {
    @Override
    public void process(T tuple)
    {
      list.add(tuple);
    }
  };
  
  @Override
  public void setup(OperatorContext context)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void beginWindow(long windowId)
  {
    list = new ArrayList<T>();
  }

  @Override
  public void endWindow()
  {
      int numEmits = topValue;
      if (isPercentage) {
        numEmits = list.size() * (topValue/100);
      }
      for (int i=0; (i < numEmits)&&(i < list.size()); i++) {
        outport.emit(list.get(i));
      }
  }

  public int getTopValue()
  {
    return topValue;
  }

  public void setTopValue(int topValue) throws Exception
  {
    if (topValue <= 0) {
      throw new Exception("Top value must be positive number.");
    }
    this.topValue = topValue;
  }

  public boolean isPercentage()
  {
    return isPercentage;
  }

  public void setPercentage(boolean isPercentage)
  {
    this.isPercentage = isPercentage;
  }

  /**
   * Output port.
   */
  public final transient DefaultOutputPort<T> outport =  new DefaultOutputPort<T>();
}
