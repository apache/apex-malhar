/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.counters.BasicCounters;
import java.io.IOException;
import org.apache.commons.lang.mutable.MutableLong;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AppDataResultAgent<T> implements Operator
{
  private MutableLong numResultsPrepared;
  private MutableLong numResultsSent;

  private transient OperatorContext context;
  private transient ObjectMapper mapper;
  private transient BasicCounters<MutableLong> appDataCounters = new BasicCounters<MutableLong>(MutableLong.class);

  public enum AppDataResultCounters
  {
    RESULTS_PREPARED,
    RESULTS_SENT;
  }

  public AppDataResultAgent()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.context = context;

    appDataCounters.setCounter(AppDataResultCounters.RESULTS_PREPARED, numResultsPrepared);
    appDataCounters.setCounter(AppDataResultCounters.RESULTS_SENT, numResultsSent);
  }

  @Override
  public void beginWindow(long windowId)
  {
    //Do nothing
  }

  public String convert(T object)
  {
    String jsonString;

    try {
      jsonString = mapper.writeValueAsString(object);
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }

    numResultsPrepared.increment();
    return jsonString;
  }

  public void sentResult()
  {
    numResultsSent.increment();
  }

  @Override
  public void endWindow()
  {
    if(context != null) {
      context.setCounters(appDataCounters);
    }
  }

  @Override
  public void teardown()
  {
    //Do nothing
  }
}
