/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBaseOutputOperator<T> extends HBaseOperatorBase implements Operator {

  private static final transient Logger logger = LoggerFactory.getLogger(HBaseOutputOperator.class);
  private List<T> tuples;

  @InputPortFieldAnnotation(name="inputPort")
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>(this) {

    @Override
    public void process(T tuple)
    {
      tuples.add(tuple);
    }

  };

  @Override
  public void setup(OperatorContext context)
  {
    tuples = new ArrayList<T>();
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    int size = tuples.size();
    for (int i = 0; i < size; ++i) {
      T t = tuples.get(i);
      try {
        processTuple(t);
      } catch (IOException e) {
        logger.error("Could not output tuple", e);
        throw new RuntimeException("Could not output tuple " + e.getMessage());
      }
    }
    tuples.clear();
  }

  public abstract void processTuple(T t) throws IOException;

}
