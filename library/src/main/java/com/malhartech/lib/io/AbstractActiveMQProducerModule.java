/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.OperatorConfiguration;
import java.util.logging.Level;
import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class AbstractActiveMQProducerModule extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQConsumerModule.class);
  public ActiveMQHelper activeMQHelper = new ActiveMQHelper(true);
  long maximumSendMessages = 0;
  long countMessages = 0;

  protected abstract Message createMessage(Object obj);

  /**
   * Do connection setup with activeMQ service.
   *
   * @param config
   * @throws FailedOperationException
   */
  @Override
  public void setup(OperatorConfiguration config)
  {
    //System.out.println("setup got called");
    activeMQHelper.setup(config);
    maximumSendMessages = config.getLong("maximumSendMessages", 0);
  }
  /**
   * Implements Sink interface.
   */
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>(this)
  {
    @Override
    public void process(Object t)
    {
      if (countMessages++ >= maximumSendMessages && maximumSendMessages != 0) {
        logger.warn("Reached maximum send messages of {}", maximumSendMessages);
        return;
      }

      //System.out.println("process got called");
      try {
        Message msg = createMessage(t);
        activeMQHelper.getProducer().send(msg);
        //System.out.println(String.format("Called process() with message %s", t.toString()));
      }
      catch (JMSException ex) {
        java.util.logging.Logger.getLogger(AbstractActiveMQProducerModule.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  };

  /**
   * Close connection attributes.
   */
  @Override
  public void teardown()
  {
    //System.out.println("teardown got called");
    activeMQHelper.teardown();
  }
}
