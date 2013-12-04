/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.integration;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;

import com.datatorrent.common.util.Slice;
import com.datatorrent.flume.operator.AbstractFlumeInputOperator;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class ApplicationTest implements StreamingApplication
{
  public static class FlumeInputOperator extends AbstractFlumeInputOperator<Slice>
  {
    @Override
    public Slice convert(byte[] buffer, int offset, int size)
    {
      return new Slice(buffer, offset, size);
    }

  }

  public static class Counter implements Operator
  {
    private int count;
    private transient Slice slice;
    public final transient DefaultInputPort<Slice> input = new DefaultInputPort<Slice>()
    {
      @Override
      public void process(Slice tuple)
      {
        count++;
        slice = tuple;
      }

    };

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
      logger.debug("total count = {}, tuple = {}", count, slice);
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(Counter.class);
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAGContext.STREAMING_WINDOW_SIZE_MILLIS, 1000);
    FlumeInputOperator flume = dag.addOperator("FlumeOperator", new FlumeInputOperator());
    flume.setConnectAddress("127.0.0.1:5033");
    Counter counter = dag.addOperator("Counter", new Counter());

    dag.addStream("Slices", flume.output, counter.input).setLocality(Locality.CONTAINER_LOCAL);
  }

  @Test
  public void test()
  {
    try {
      LocalMode.runApp(this, Integer.MAX_VALUE);
    }
    catch (Exception ex) {
      logger.warn("The dag seems to be not testable yet, if it's - remove this exception handling", ex);
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(ApplicationTest.class);
}
