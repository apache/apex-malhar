/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.performance;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class DoNothingModule<T> extends BaseOperator
{
  private static final long serialVersionUID = 201208061821L;
  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>(this);
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      output.emit(tuple);
    }
  };
}
