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
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBaseOutputOperator<T> extends HBaseOperatorBase implements Operator {

  private static final transient Logger logger = LoggerFactory.getLogger(HBaseOutputOperator.class);
  protected static final int DEFAULT_BATCH_SIZE = 1000;

  private List<T> tuples;
  private int batchSize = DEFAULT_BATCH_SIZE;
  private int tupleCount;

  @InputPortFieldAnnotation(name="inputPort")
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>(this) {

    @Override
    public void process(T tuple)
    {
      tuples.add(tuple);
      if (++tupleCount >= batchSize) {
        processTuples();
      }
    }

  };

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  @Override
  public void setup(OperatorContext context)
  {
    tuples = new ArrayList<T>();
    tupleCount = 0;
    setupConfiguration();
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
    processTuples();
  }

  private void processTuples() {
    Iterator<T> it = tuples.iterator();
    while (it.hasNext()) {
      T t = it.next();
      try {
        processTuple(t);
        --tupleCount;
      } catch (IOException e) {
        logger.error("Could not output tuple", e);
        throw new RuntimeException("Could not output tuple " + e.getMessage());
      }
      it.remove();
    }
  }

  public abstract void processTuple(T t) throws IOException;

}
