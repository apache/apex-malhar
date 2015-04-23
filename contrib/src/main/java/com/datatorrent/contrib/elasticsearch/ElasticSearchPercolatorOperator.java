/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.elasticsearch;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.elasticsearch.action.percolate.PercolateResponse;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.DTThrowable;

/**
 * Percolate operator for ElasticSearch
 * 
 */
public class ElasticSearchPercolatorOperator extends BaseOperator
{
  @NotNull
  public String hostName;
  public int port;
  
  @NotNull
  public String indexName;
  @NotNull
  public String documentType;

  protected transient ElasticSearchPercolatorStore store;
  public final transient DefaultOutputPort<PercolateResponse> outputPort = new DefaultOutputPort<PercolateResponse>();

  public final transient DefaultInputPort<Object> inputPort = new DefaultInputPort<Object>() {

    /*
     * (non-Javadoc)
     * 
     * @see com.datatorrent.api.DefaultInputPort#process(java.lang.Object)
     */
    @Override
    public void process(Object tuple)
    {

      PercolateResponse response = store.percolate(new String[] { indexName }, documentType, tuple);
      outputPort.emit(response);
    }
  };

  public void setup(com.datatorrent.api.Context.OperatorContext context)
  {
    store = new ElasticSearchPercolatorStore(hostName, port);
    try {
      store.connect();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }
  
  /* (non-Javadoc)
   * @see com.datatorrent.api.BaseOperator#teardown()
   */
  @Override
  public void teardown()
  {
    super.teardown();
    try {
      store.disconnect();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }
}
