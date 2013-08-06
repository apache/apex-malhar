package com.datatorrent.lib.pigquery;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

public abstract class PigSplitOperator<T>  extends BaseOperator
{
  /**
   * Number of output ports.
   */
  private int numOutPorts = 1;
  
  /**
   * Constructor
   */
  public PigSplitOperator(int numOutPorts) {
    if (numOutPorts > 1) this.numOutPorts = numOutPorts;
    for (int i=0; i < numOutPorts; i++) {
      outports.add(new DefaultOutputPort<T>());
    }
  }
  
  /**
   * Input port.
   */
  public final transient DefaultInputPort<T> inport = new DefaultInputPort<T>() {
    @Override
    public void process(T tuple)
    {
      for (int i=0; i < numOutPorts; i++) {
        if (isValidEmit(i, tuple)) {
          outports.get(i).emit(tuple);
        }
      }
    }
   };

  abstract public boolean isValidEmit(int i, T tuple);
  
  /**
   * Output port.
   */
  public transient List<DefaultOutputPort<T>> outports = new ArrayList<DefaultOutputPort<T>>();
}
