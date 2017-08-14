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
package org.apache.apex.malhar.lib.converter;

import java.io.UnsupportedEncodingException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

public class ByteArrayToStringConverterTest
{
  @Test
  public void testByteArrayToStringConversion() throws UnsupportedEncodingException
  {
    ByteArrayToStringConverterOperator testop = new ByteArrayToStringConverterOperator();
    String test1 = "hello world with UTF8";
    byte[] utf8Bytes = test1.getBytes("UTF-8");
    String test2 = "hello world@#'!!!.: with UTF-16";
    byte[] asciiBytes = test2.getBytes("UTF-16");
    CollectorTestSink<String> testsink = new CollectorTestSink<String>();
    TestUtils.setSink(testop.output, testsink);
    testop.beginWindow(0);
    testop.setCharacterEncoding("UTF-8");
    testop.input.put(utf8Bytes);
    testop.setCharacterEncoding("UTF-16");
    testop.input.put(asciiBytes);
    testop.endWindow();

    Assert.assertEquals(2,testsink.collectedTuples.size());
    for (String output: testsink.collectedTuples) {
      logger.debug("output is {}",output);
      Assert.assertEquals(test1, output);
      test1 = test2;
    }
  }

  @Test
  public void testByteArrayToStringConversionDefaultEncoding() throws UnsupportedEncodingException
  {
    ByteArrayToStringConverterOperator testop = new ByteArrayToStringConverterOperator();
    String test1 = "hello world with default encoding";
    byte[] bytes = test1.getBytes();
    CollectorTestSink<String> testsink = new CollectorTestSink<String>();
    TestUtils.setSink(testop.output, testsink);
    testop.beginWindow(0);
    testop.input.put(bytes);
    testop.endWindow();

    Assert.assertEquals(1,testsink.collectedTuples.size());
    Assert.assertEquals(test1, testsink.collectedTuples.get(0));

  }

  private static final Logger logger = LoggerFactory.getLogger(ByteArrayToStringConverterTest.class);
}
