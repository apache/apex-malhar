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
package org.apache.apex.malhar.flume.interceptor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;

import static org.junit.Assert.assertArrayEquals;

/**
 *
 */
public class ColumnFilteringInterceptorTest
{
  private static InterceptorTestHelper helper;

  @BeforeClass
  public static void startUp()
  {
    HashMap<String, String> contextMap = new HashMap<String, String>();
    contextMap.put(ColumnFilteringInterceptor.Constants.DST_SEPARATOR, Byte.toString((byte)1));
    contextMap.put(ColumnFilteringInterceptor.Constants.SRC_SEPARATOR, Byte.toString((byte)2));
    contextMap.put(ColumnFilteringInterceptor.Constants.COLUMNS, "1 2 3");

    helper = new InterceptorTestHelper(new ColumnFilteringInterceptor.Builder(), contextMap);
  }

  @Test
  public void testInterceptEvent()
  {
    helper.testIntercept_Event();
  }

  @Test
  public void testFiles() throws IOException, URISyntaxException
  {
    helper.testFiles();
  }

  @Test
  public void testInterceptEventWithColumnZero()
  {
    HashMap<String, String> contextMap = new HashMap<String, String>();
    contextMap.put(ColumnFilteringInterceptor.Constants.DST_SEPARATOR, Byte.toString((byte)1));
    contextMap.put(ColumnFilteringInterceptor.Constants.SRC_SEPARATOR, Byte.toString((byte)2));
    contextMap.put(ColumnFilteringInterceptor.Constants.COLUMNS, "0");

    ColumnFilteringInterceptor.Builder builder = new ColumnFilteringInterceptor.Builder();
    builder.configure(new Context(contextMap));
    Interceptor interceptor = builder.build();

    assertArrayEquals("Empty Bytes",
        "\001".getBytes(),
        interceptor.intercept(new InterceptorTestHelper.MyEvent("".getBytes())).getBody());

    assertArrayEquals("One Field",
        "First\001".getBytes(),
        interceptor.intercept(new InterceptorTestHelper.MyEvent("First".getBytes())).getBody());

    assertArrayEquals("Two Fields",
        "\001".getBytes(),
        interceptor.intercept(new InterceptorTestHelper.MyEvent("\002First".getBytes())).getBody());
  }
}
