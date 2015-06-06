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
package com.datatorrent.contrib.rabbitmq;

import java.io.IOException;
import java.net.MalformedURLException;

import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RabbitMQOutputOperatorBenchmark extends RabbitMQOutputOperatorTest
{
  @Test
  public void testDag() throws InterruptedException, MalformedURLException, IOException, Exception
  {
    logger = LoggerFactory.getLogger(RabbitMQOutputOperatorTest.class);
    final int testNum = 100000;
    runTest(testNum);
    
    logger.debug("end of test");  }
}
