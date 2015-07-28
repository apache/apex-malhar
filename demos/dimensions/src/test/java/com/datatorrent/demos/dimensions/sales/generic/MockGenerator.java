/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.sales.generic;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.demos.dimensions.InputGenerator;

import static com.datatorrent.demos.dimensions.sales.generic.JsonSalesGenerator.*;

public class MockGenerator implements InputGenerator<byte[]>
{
  private static final ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);

  public final transient DefaultOutputPort<byte[]> jsonBytes = new DefaultOutputPort<byte[]>();
  private boolean sent = false;

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void emitTuples()
  {
    if(sent) {
      return;
    }

    sent = true;

    LOG.debug("Generating Mock data");

    SalesEvent salesEvent = new SalesEvent();
    salesEvent.time = System.currentTimeMillis();
    salesEvent.productId = 1;
    salesEvent.channel = "Mobile";
    salesEvent.region = "Atlanta";
    salesEvent.sales = 1.0;
    salesEvent.tax = 2.0;
    salesEvent.discount = 3.0;

    try {
      jsonBytes.emit(mapper.writeValueAsBytes(salesEvent));
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public OutputPort<byte[]> getOutputPort()
  {
    return jsonBytes;
  }

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MockGenerator.class);
}
