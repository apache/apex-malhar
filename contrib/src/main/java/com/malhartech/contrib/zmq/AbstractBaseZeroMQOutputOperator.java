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
 * ZeroMQ output adapter operator, which send data to ZeroMQ message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>url</b>:the url for the publisher to listen to subscriber <br>
 * <b>syncUrl</b>: the url for the publisher to synchronize with subscriber<br>
 * <b>SUBSCRIBERS_EXPECTED</b>: the expected number of subscribers<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractBaseZeroMQOutputOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractBaseZeroMQInputOperator.class);
  transient protected ZMQ.Context context;
  transient protected ZMQ.Socket publisher;
  transient protected ZMQ.Socket syncservice;
  protected int SUBSCRIBERS_EXPECTED = 1;
  private String url;
  @InjectConfig(key = "syncUrl")
  private String syncUrl;
  protected boolean syncStarted = false;

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
    logger.debug("O/P setup");
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
    syncStarted = true;
  }

  @Override
  public void teardown()
  {
    publisher.close();
    context.term();
  }
}
