/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.ads.generic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.demos.dimensions.InputGenerator;
import com.datatorrent.demos.dimensions.ads.AdInfo;

public class MockGenerator implements InputGenerator<AdInfo>
{
  private boolean sent = false;

  public final transient DefaultOutputPort<AdInfo> outputPort = new DefaultOutputPort<AdInfo>();

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

    AdInfo adinfo = new AdInfo();

    adinfo.advertiser = "safeway";
    adinfo.location = "SKY";
    adinfo.publisher = "google";

    adinfo.impressions = 5;
    adinfo.clicks = 10;
    adinfo.cost = 5.0;
    adinfo.revenue = 3.0;
    adinfo.time = 60000;

    LOG.debug("emitting data");
    outputPort.emit(adinfo);

    sent = true;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public OutputPort<AdInfo> getOutputPort()
  {
    return outputPort;
  }

  private static final Logger LOG = LoggerFactory.getLogger(MockGenerator.class);
}
