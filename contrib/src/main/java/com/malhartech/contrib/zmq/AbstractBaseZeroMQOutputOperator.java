/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import com.malhartech.annotation.InjectConfig;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractBaseZeroMQOutputOperator<T> extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractBaseZeroMQInputOperator.class);
  protected ZMQ.Context context;
  protected ZMQ.Socket publisher;
  protected ZMQ.Socket syncservice;
  @InjectConfig(key = "url")
  private String url;
  @InjectConfig(key = "syncUrl")
  private String syncUrl;
  @InjectConfig(key = "SUBSCRIBERS_EXPECTED")
  protected int SUBSCRIBERS_EXPECTED;

  public void setUrl(String url)
  {
    this.url = url;
  }

  public void setSyncUrl(String syncUrl)
  {
    this.syncUrl = syncUrl;
  }

  public void setSUBSCRIBERS_EXPECTED(int expected)
  {
    SUBSCRIBERS_EXPECTED = expected;
  }

  public int getSUBSCRIBERS_EXPECTED()
  {
    return SUBSCRIBERS_EXPECTED;
  }

  @Override
  public void setup(OperatorContext ctx)
  {
    context = ZMQ.context(1);
    publisher = context.socket(ZMQ.PUB);
    publisher.bind(url);
    syncservice = context.socket(ZMQ.REP);
    syncservice.bind(syncUrl);
  }
  // necessary for publisher side to synchronize publisher and subscriber, must run after setup()

  public void startSyncJob()
  {
    for (int subscribers = 0; subscribers < SUBSCRIBERS_EXPECTED; subscribers++) {
      byte[] value = syncservice.recv(0);
      syncservice.send("".getBytes(), 0);
    }
  }

  @Override
  public void teardown()
  {
    publisher.close();
    context.term();
  }
}
