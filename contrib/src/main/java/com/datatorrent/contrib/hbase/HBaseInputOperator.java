/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.hbase;

import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import java.io.IOException;

/**
 * The base class for HBase input operators.<br>
 *
 * <br>
 * @param <T> The tuple type
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBaseInputOperator<T> extends HBaseOperatorBase implements InputOperator
{

  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  //protected abstract T getTuple(Result result);
  //protected abstract T getTuple(KeyValue kv);

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    try{
      setupConfiguration();
    } catch (IOException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public void teardown()
  {
  }

}
