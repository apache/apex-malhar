/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.stream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.EventGenerator;

/**
 * Functional tests for {@link org.apache.apex.malhar.lib.testbench.DevNull}.
 */
public class DevNullTest
{

  private static Logger LOG = LoggerFactory.getLogger(EventGenerator.class);

  /**
   * Tests both string and non string schema
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testSingleSchemaNodeProcessing() throws Exception
  {
    DevNull oper = new DevNull();

    oper.beginWindow(0);
    long numtuples = 1000000;
    Object o = new Object();
    for (long i = 0; i < numtuples; i++) {
      oper.data.process(o);
    }
    oper.endWindow();
    LOG.info(String.format("\n*******************************************************\nnumtuples(%d)", numtuples));
  }
}
