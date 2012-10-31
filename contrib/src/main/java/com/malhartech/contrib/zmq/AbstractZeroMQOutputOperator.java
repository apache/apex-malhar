/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.zmq;

import com.malhartech.api.DefaultInputPort;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractZeroMQOutputOperator<T> extends AbstractBaseZeroMQOutputOperator
{

  /**
   * Users need to provide conversion of Tuple to message
   * @param tuple
   */
  public abstract void processTuple(T tuple);

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple); // This is an abstract call
    }
  };
}
