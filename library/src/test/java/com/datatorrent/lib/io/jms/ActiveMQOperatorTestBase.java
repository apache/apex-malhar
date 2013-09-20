/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.jms;

import java.io.File;

import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for Active MQ operators test
 * Setup the Active MQ service to serve the test
 */
public class ActiveMQOperatorTestBase
{

  private BrokerService broker;

  public ActiveMQOperatorTestBase()
  {
    super();
  }

  /**
   * Start ActiveMQ broker from the Testcase.
   *
   * @throws Exception
   */
  private void startActiveMQService() throws Exception
  {
    broker = new BrokerService();
    String brokerName = "ActiveMQOutputOperator-broker";
    broker.setBrokerName(brokerName);
    broker.getPersistenceAdapter().setDirectory(new File("target/test-data/activemq-data/" + broker.getBrokerName() + "/KahaDB"));
    broker.addConnector("tcp://localhost:61617?broker.persistent=false");
    broker.getSystemUsage().getStoreUsage().setLimit(1024 * 1024 * 1024);  // 1GB
    broker.getSystemUsage().getTempUsage().setLimit(100 * 1024 * 1024);    // 100MB
    broker.setDeleteAllMessagesOnStartup(true);
    broker.start();
  }

  @Before
  public void beforTest() throws Exception
  {
    startActiveMQService();
  }

  @After
  public void afterTest() throws Exception
  {
    broker.stop();
  }

}